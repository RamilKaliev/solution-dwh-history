package app

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, Row, SparkSession}

object DwhHistorySAUtils extends DwhHistoryUtils {

  def processByReduce(path: String, targetDir: String = "savings_accounts")(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val sourceDf = readJson(s"$path/$targetDir")

    val preparedDF = prepare(sourceDf)
      .sort(col("id").asc, col("ts").asc)
      .groupByKey(_.getAs[String]("id"))

    reduce(preparedDF)
      .toDF("group_id", "state")
      .selectExpr(
        "state.id as id",
        "state.ts as ts",
        "state.data as data",
        "state.savings_account_id as savings_account_id"
      )
  }


  def prepare(df: DataFrame): DataFrame = df
    .selectExpr(
      "to_json(data) as data",
      "id",
      "op",
      "to_json(set) as set",
      "ts",
      "cast(null as string) as savings_account_id"
    )


  def reduce(df: KeyValueGroupedDataset[String, Row]): Dataset[(String, Row)] = df
    .reduceGroups((a, b) => restoreSAByReduceFunction(a, b))


  def restoreSAByReduceFunction(a: Row, b: Row): Row = {
    // merge 'data' and 'set'
    val newDataMap = mergeDataAndSet(a, b)
    val newDataJson = mapToJString(newDataMap)

    // reduced row
    Row(
      newDataJson,
      a.get(1),
      a.get(2),
      null,
      b.get(4),
      newDataMap.getOrElse("savings_account_id", null)
    )
  }

}
