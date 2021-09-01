package app

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.{JsonMethods, Serialization}

/**
 * Consists of utilities for any related needs
 */
trait DwhHistoryUtils {

  implicit val formats = DefaultFormats

  def readJson(path: String)(implicit sparkSession: SparkSession): DataFrame = sparkSession
    .read
    .format("json")
    .options(Map("inferSchema" -> "true", "multiLine" -> "true", "path" -> path))
    .load()

  def jsonizeTextAsMap(jstring: String): Map[String, Any] = JsonMethods.parse(jstring).extract[Map[String, Any]]

  def mapToJString(map: Map[String, Any]) = Serialization.write(map)

  // merge 2 JSON structures
  def mergeDataAndSet(a: Row, b: Row, dataIdx: Int = 0, setIdx: Int = 3): Map[String, Any] = {
        // get a and b 'data' and 'set' fields and make a json from it
    val aData = a.getString(dataIdx)
    val aSet = a.getString(setIdx)
    val bData = b.getString(dataIdx)
    val bSet = b.getString(setIdx)

    // update 'data' with values from 'set'
    val (data, set) = if (aData != null && bSet != null) {
      (aData, bSet)
    } else if (bData != null && aSet != null) {
      (bData, aSet)
    } else {
      if (aData != null && aSet != null) {
        (aData, aSet)
      } else {
        (bData, bSet)
      }
    }

    // jsonize
    val dataMap = jsonizeTextAsMap(data)
    val setMap = jsonizeTextAsMap(set)
    // merge
    dataMap ++ setMap
  }

}
