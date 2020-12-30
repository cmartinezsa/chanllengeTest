package com.cms.challenge.operator

import com.cms.challenge.common.Atributos
import com.cms.challenge.etl.StockPrices
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

import com.cms.challenge.common.Args

object ETLJobOperator extends Atributos {
  private val LOGGER: Logger = LogManager.getLogger(msjLog)
  val STOCK_PRICES = new StockPrices()

  def main(args: Array[String]): Unit = {
    println("INICIANDO PROCESO")
    val SPARK_SESSION = initializeSparkSession(args)
    if (Args.checkParams.equals(false)) {
      LOGGER.info("FINALIZA EJECUCION ANTICIPADA")
      SPARK_SESSION.stop()
    } else {
      LOGGER.info("VALIDACION DE PAMETROS EXITOSO")
      STOCK_PRICES.stockPricesETL(Args.pathFile, Args.datePart, Args.tickerList, Args.numDay)(SPARK_SESSION)
      SPARK_SESSION.stop()
    }
    LOGGER.info("*** FINISHED ****")
  }

  /**
    *
    * @param args
    * @return SparkSession for orchestrate the process.
    */
  def initializeSparkSession(args: Array[String]): SparkSession = {
    LOGGER.info("Initializing SparkSession")
    Args.getArguments(args)
    if (Args.checkParams.equals(true)) {
      SparkSession
        .builder()
        .config("spark.service.user.postgresql.pass", "cmsdb")
        .config("spark.service.user.postgresql.user", "postgres")
        .config("spark.service.user.postgresql.database", "challengedb")
        .config("spark.service.user.postgresql.port", "5432")
        .config("spark.service.user.postgresql.host", "localhost")
        .master(Args.modeExecution)
        .appName(Args.processName)
        .getOrCreate()
    }
    else {
      SparkSession
        .builder()
        .master("local")
        .appName("Process Failed")
        .getOrCreate()
    }
  }
}

