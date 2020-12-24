package com.cms.challenge.operator

import com.cms.challenge.common.Atributos
import com.cms.challenge.etl.EtlTableCargo
import com.cms.challenge.etl.StockPrices
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

import com.cms.challenge.common.Args

object ETLJobOperator extends Atributos {
  private val LOGGER: Logger = LogManager.getLogger(msjLog)
  val ETLCARGO = new EtlTableCargo()
  val STOCK_PRICES = new StockPrices()


  def main(args: Array[String]): Unit = {
    println("INICIANDO PROCESO")
    val SPARK = initializeSparkSession(args)
    println("INICIANDO SPARK")
    if (Args.checkParamsBoolean.equals(false)) {
      LOGGER.info("FINALIZA EJECUCION ANTICIPADA")
      SPARK.stop()
    } else {
      LOGGER.info("VALIDACION DE PAMETROS EXITOSO")
      STOCK_PRICES.stockPricesETL(Args.pathFile, Args.datePart, Args.tickerList, Args.numDay)(SPARK)
      SPARK.stop()
    }
    LOGGER.info("*** FINISHED ****")
  }

  /**
    *
    * @param args
    * @return
    */
  def initializeSparkSession(args: Array[String]): SparkSession = {
    LOGGER.info("Iniciando Proceso")
    println("INICIANDO INITIALIZED")
    Args.getArguments(args)
    println("initialized Mode : " + Args.modeExecution)
    println("Initialized " + Args.processName)

    if (Args.checkParamsBoolean.equals(true)) {
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

