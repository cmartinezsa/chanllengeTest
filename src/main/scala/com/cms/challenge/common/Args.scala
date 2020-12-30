package com.cms.challenge.common

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

import com.cms.challenge.etl.StockPrices
import java.lang.ArrayIndexOutOfBoundsException


object Args extends Atributos {

  val logger: Logger = LogManager.getLogger("ETL - Data Pipeline - Test")
  var target = ""

  /**
    *
    * @param args received
    * @return Booolean value
    */
  def getArguments(args: Array[String]): Boolean = {
    try {
      println(args.length)
      pathFile = args(0)
      datePart = args(1)
      tickerList = args(2).split(",").map(_.trim).toList
      numDay = args(3).toInt
      processName = args(4)
      modeExecution = args(5)
      target = args(6)

      if (args.length < 7) {
        logger.info("Check number of params, required 7 arguments")
        checkParams
      }
      else {
        logger.info(s"Parameters received => pathFile : $pathFile datePart: $datePart" +
          s" TickerList: $tickerList NumDay: $numDay ProcessName: $processName " +
          s" ModeExecution: $modeExecution  Target: $target")
        checkParams = true
        return checkParams
      }
    }
    catch {
      case e: ArrayIndexOutOfBoundsException =>
        logger.info("required 7 parameters " + e)
        println("required 7  parameters " + e)
        checkParams
      case _ =>
        checkParams
    }
  }
}
