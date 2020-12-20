package com.cms.challenge.common

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/**
 *
 */
class Reader extends Atributos {
  val logger: Logger = LogManager
    .getLogger("ETL - Data Pipeline - Test")
  /**
   *
   * @param pathFile
   * @param spark
   * @return
   */
  def readCSVFile(pathFile: String)(implicit spark: SparkSession): DataFrame = {
    val dfEmpty=spark.emptyDataFrame
    try {
      spark.read
        .option("inferSchema", "true") // Make sure to use string version of true
        .option("header", true)
        .option("dateFormat", "yyyy-MM-dd")
        .option("sep", ",")
        .csv(pathFile)

    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }
}
