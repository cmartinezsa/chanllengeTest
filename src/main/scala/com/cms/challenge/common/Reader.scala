package com.cms.challenge.common

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}


/**
  *
  */
class Reader extends Atributos {
  val logger: Logger = LogManager.getLogger("ETL - Data Pipeline - Test")
  val filter = new Filter()

  /**
    *
    * @param pathFile
    * @param spark
    * @return
    */
  def readCSVFile(pathFile: String, date_part: String, ticker: List[String], numDay: Int)
                 (implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame
    try {
      val df = spark.read
        .option("inferSchema", "true") // Make sure to use string version of true
        .option("header", true)
        .option("dateFormat", "yyyy-MM-dd")
        .option("sep", ",")
        .csv(pathFile)

      println("readCSVFile, read Finished, continue Filter")

      val dfFiltered = filter.getFilteredOperations(df, ticker, numDay, date_part)
        .persist()

      val dfShow=dfFiltered.count()
        println("Numero de registros  " + dfShow)

      if (!dfFiltered.isEmpty){
        println("**** EL DATA FRAME FILTRADO SI CONTIENE DATOS ")
        dfFiltered

      }else{
        println("**** EL DATA FRAME FILTRADO NO CONTIENE DATOS ")
        dfEmpty
      }

    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }

}
