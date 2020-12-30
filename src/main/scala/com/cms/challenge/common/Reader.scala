package com.cms.challenge.common

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

class Reader extends Atributos {
  val logger: Logger = LogManager.getLogger("ETL - Data Pipeline - Test")
  val filter = new Filter()

  /**
    *
    * @param pathFile from origin the dataset csv.
    * @param spark    Active
    * @return DataFrame with the data.
    */
  def readCSVFile(pathFile: String, datePart: String, ticker: List[String], numDay: Int)
                 (implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame
    try {
      val df = spark.read
        .option("inferSchema", "true") // Make sure to use string version of true
        .option("header", true)
        .option("dateFormat", "yyyy-MM-dd")
        .option("sep", ",")
        .csv(pathFile)

      val dfFiltered = filter.getFilteredOperations(df, ticker, numDay, datePart)
      dfFiltered
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }

  def readParquetFile(pathFileParquet: String)(implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame
    try {
      val df = spark
        .read
        .parquet(pathFileParquet)
      df
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }
}
