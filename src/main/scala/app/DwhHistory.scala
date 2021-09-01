package app

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

object DwhHistory extends DwhHistoryUtils {

  val logger = Logger(LoggerFactory.getLogger(getClass))

  def main(args: Array[String]): Unit = {

    logger.info(s" === Arguments = $args === ")

    if (args.isEmpty || args.length < 2) {
      logger.error("mainPath and scdType arguments must be specified")
      throw new Exception("mainPath and scdType arguments must be specified")
    }

    val mainPath = args(0)
    val scdType = args(1)

    run(mainPath, scdType)

  }


  def run(mainPath: String = "data/dwh", scdType: String): Unit = {

    logger.info(s" === MainPath = $mainPath, SCDType = $scdType === ")

    implicit val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DwhHistory")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // discover data
    discoverData(mainPath, Array("accounts", "cards", "savings_accounts"))

    // choose between scd types
    scdType match {
      case "type1" => scd1(mainPath)
      case "type2" => scd2(mainPath)
      case _ => {
        logger.error(s" === There is no solution for SCDType=${scdType} === ")
        throw new IllegalArgumentException(s"There is no solution for SCDType=${scdType}")
      }
    }

  }


  // shows each domain in a sorted way by 'id' and 'ts' with 'ts' convertion into 'dt'
  def discoverData(mainPath: String, domains: Array[String])(implicit sparkSession: SparkSession): Unit = {
    domains.foreach(domain => {
      logger.info(s" === Discover domain='${domain}' === ")
      readJson(s"$mainPath/$domain")
        .selectExpr(
          "id",
          "op",
          "data.*",
          "to_utc_timestamp(from_unixtime(ts/1000, \"yyyy-MM-dd HH:mm:ss\"),\"UTC\") as dt",
          "set.*"
        )
        .sort(col("id").asc, col("ts").asc)
        .show(false)
    })
  }


  /*
  SCD Type 1 - "Replay" state from the set of events with override
   */
  def scd1(mainPath: String)(implicit sparkSession: SparkSession): Unit = {

    logger.info(" === SCD Type 1 has STARTED === ")
    logger.info(" ============================== ")

    // restore accounts
    logger.info(" === ACCOUNTS === ")
    val restoredAccountsDF = DwhHistoryAccountsUtils.processByReduce(mainPath)
    restoredAccountsDF.cache()
    restoredAccountsDF.show(false)

    // restore cards
    logger.info(" === CARDS === ")
    val restoredCardsDF = DwhHistoryCardsUtils.processByReduce(mainPath)
    restoredCardsDF.cache()
    restoredCardsDF.show(false)

    // restore savings accounts
    logger.info(" === SAVINGS ACCOUNT === ")
    val restoredSaDF = DwhHistorySAUtils.processByReduce(mainPath)
    restoredSaDF.cache()
    restoredSaDF.show(false)

    // restored JOIN between tables
    logger.info(" === JOIN === ")
    val join1 = restoredAccountsDF
      .join(
        restoredCardsDF,
        restoredAccountsDF.col("card_id") === restoredCardsDF.col("card_id")
      )
    val join2 = join1.join(
      restoredSaDF,
      join1.col("savings_account_id") === restoredSaDF.col("savings_account_id")
    )
    join2.show(false)

    logger.info(" === SCD Type 1 has FINISHED === ")
    logger.info(" =============================== ")
  }


  /*
  SCD Type 2 - Iterate through and make the historical records
  NOTE: Not an elegant way of dropping empty field but just only for the case of usage a general approach here.
   */
  def scd2(mainPath: String)(implicit sparkSession: SparkSession): Unit = {

    logger.info(" === SCD Type 2 has STARTED === ")
    logger.info(" ============================== ")

    // restore accounts
    logger.info(" === ACCOUNTS === ")
    val restoredAccountsDF = processByIteration(mainPath, "accounts")
    restoredAccountsDF.cache()
    restoredAccountsDF.show(false)

    // restore cards
    logger.info(" === CARDS === ")
    val restoredCardsDF = processByIteration(mainPath, "cards").drop("savings_account_id")
    restoredCardsDF.cache()
    restoredCardsDF.show(false)

    // restore savings accounts
    logger.info(" === SAVINGS ACCOUNT === ")
    val restoredSaDF = processByIteration(mainPath, "savings_accounts").drop("card_id")
    restoredSaDF.cache()
    restoredSaDF.show(false)

    // JOIN between tables
    logger.info(" === JOIN === ")

    // accounts with cards
    restoredAccountsDF.as("a")
      .join(
        restoredCardsDF.as("c"),
        col("a.card_id") === col("c.card_id") && (col("a.eff_from") === col("c.eff_from"))
      ).show( false)

    // accounts with savings_accounts
    restoredAccountsDF.as("a").join(
      restoredSaDF.as("sa"),
      col("a.savings_account_id") === col("sa.savings_account_id") && (col("a.eff_from") === col("sa.eff_from"))
    ).show( false)

    logger.info(" === SCD Type 2 has FINISHED === ")
    logger.info(" =============================== ")
  }


  /*
  SCD2 type
   */
  def processByIteration(path: String, targetDir: String)(implicit sparkSession: SparkSession): DataFrame = {
    /*
    Read and Prepare.
    Steps are:
    - Convert dictionary to JSON - for the further map conversion
    - Prepare structure with effective dates and current record flag to track relevance
     */
    val sourceDF = readJson(s"$path/$targetDir")
      .selectExpr("to_json(data) as data", "id", "op", "to_json(set) as set", "ts")
      .select(
        col("id"),
        when(col("data").isNull, lit(null).cast(StringType)).otherwise(col("data")).as("data"),
        col("ts"),
        to_utc_timestamp(from_unixtime(col("ts")/1000, "yyyy-MM-dd HH:mm:ss"),"UTC").as("eff_from"),
        lit("2999-12-31").cast(TimestampType).as("eff_to"),
        lit("1").cast(IntegerType).as("is_current"),
        when(col("set").isNull, lit(null).cast(StringType)).otherwise(col("set")).as("set"),
      )
      .sort("id", "ts")

    sourceDF.cache()

    /*
    cascading iteration until complete history is met.
    Steps are:
    - repartition by id field (partitions number equal to 1 because small amount of data)
    - process each partition with building a dictionary with 'id' field as a 'key' and sequence of records is 'value'
     */
    cascadeIteration(sourceDF)

  }


  def cascadeIteration(sourceDF: DataFrame): DataFrame = {
    // general historical schema
    val schema = StructType(Seq(
      StructField("id", DataTypes.StringType, false),
      StructField("data", DataTypes.StringType, false),
      StructField("eff_from", DataTypes.TimestampType, false),
      StructField("eff_to", DataTypes.TimestampType, false),
      StructField("is_current", DataTypes.IntegerType, false),
    ))
    implicit val rowEncoder = RowEncoder(schema)

    val resultDF = sourceDF.repartition(1, col("id")).mapPartitions((it: Iterator[Row]) => {

      // NOTE: In a way of a huge partition - list buffer will grow.
      // In case of that - code can be optimized and some Spill-To-Disk structure can be used
      val map = collection.mutable.Map.empty[String, collection.mutable.ListBuffer[Row]]

      // Walk through the partition and build a "dictionary" of (id, list of records by that id)
      it.foreach(row => {
        val id = row.getString(0)

        // take a 'set' structure
        val setMap = if (row.isNullAt(6)) Map.empty[String, Any] else jsonizeTextAsMap(row.getString(6))

        // cascading between edge and further records to make eff_from and eff_to relevant
        val (dataMap: Map[String, Any], list: collection.mutable.ListBuffer[Row]) = if (map.contains(id)) {
          map.get(id).map(list => {
            // update previous row - set eff_to, is_current
            val lastRow = list.last
            list.update(list.length-1, Row(lastRow.get(0), lastRow.get(1), lastRow.get(2), row.get(3), 0))
            // prepare for the current row
            val dataMap = jsonizeTextAsMap(lastRow.getString(1))
            (dataMap, list)
          }).get
        } else {
          // if there is no data in 'data' field - take it as empty Map
          val dataMap = if (row.isNullAt(1)) Map.empty[String, Any] else jsonizeTextAsMap(row.getString(1))
          val list = collection.mutable.ListBuffer.empty[Row]
          map.+=((id, list))
          (dataMap, list)
        }

        // combine 'data' and 'set'
        val newDataMap = dataMap ++ setMap
        val newDataJson = mapToJString(newDataMap)
        // restructure into a new row
        val newRow = Row(id, newDataJson, row.get(3), row.get(4), 1)
        list.append(newRow)
      })
      // return an iterator of all records
      map.values.flatten.toIterator
    })(rowEncoder)


    // extended schema with card_id, savings_account_id - this is for a specific JOIN between all dimension tables
    implicit val extendedRowEncoder = RowEncoder(
      schema
        .add(StructField("card_id", StringType))
        .add(StructField("savings_account_id", StringType))
    )

    // walk through the records and extend each one of them
    resultDF.map(row => {
      val dataMap = jsonizeTextAsMap(row.getString(1))
      val newRow = Row(row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), dataMap.getOrElse("card_id", null), dataMap.getOrElse("savings_account_id", null))
      newRow
    })(extendedRowEncoder)
  }

}
