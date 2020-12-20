package com.cms.challenge.etl

import com.cms.challenge.common.Reader
import org.apache.spark.sql.{AnalysisException, SparkSession}

import org.apache.log4j.{LogManager, Logger}

class StockPrices {
  val reader = new Reader()
  val logger: Logger = LogManager
    .getLogger("ETL - Data Pipeline - Test")

  def stockPricesETL(pathFile: String, date_part: String)(implicit spark: SparkSession): Unit = {
    try {
      val dfCargo = reader.readCSVFile(pathFile)
        .select("*")
        .dropDuplicates()

      // Imprimir el schema
      val dfSelect = dfCargo.printSchema()
      // Mirar una pequena muestra de los datos..
      dfCargo.show(20, false)

    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
    }

  }
}
