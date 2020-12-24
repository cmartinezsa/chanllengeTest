package com.cms.challenge.common

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, date_sub, lit, max, min, substring, to_date}
import org.apache.spark.storage.StorageLevel

class Filter extends Atributos {
  val logger: Logger = LogManager.getLogger("ETL - Data Pipeline - Test")

  def getFilteredOperations(df: DataFrame, ticker: List[String], days: Int, date_part: String)
                           (implicit spark: SparkSession): DataFrame = {
    println("metodo getFilteredOperations : " + ticker + " days " + days)

    val dfEmpty = spark.emptyDataFrame
    try {
      val datePointExtract = getDateToCompute(df, date_part, days)(spark)

      println("Filtrar por  : " + datePointExtract + " AND " + date_part)

     val dfFilter= df.filter(col(dateField).between(datePointExtract, date_part))

      val dfFilterShow=dfFilter
      dfFilterShow.printSchema()


      dfFilter .filter(col(tickerField) isin (ticker: _*))

      val dfFilterShowTickers=dfFilter
      dfFilterShowTickers.show(100, false)

      dfFilter


    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }

  def getDateToCompute(df: DataFrame, date: String, days: Int)(implicit spark: SparkSession): String = {

    println(" Encontrar la fecha inicial: " + date + " MENOS " + days + " dias")
    //val dateToExtract = df.selectExpr(s"(date_sub((lit($date)), $days)) as $dateStartToExtract")
    val dateToExtract = df.select(date_sub(lit(date), days).alias("date_sub_start"))
      .select(col("date_sub_start"))
      .first().getDate(0).toString
    logger.info(s"Get date to extract data : " + dateToExtract)

    println("Get date to extract data " + dateToExtract)
    return dateToExtract
  }
}
