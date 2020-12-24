package com.cms.challenge.common

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

import com.cms.challenge.etl.StockPrices
import java.lang.ArrayIndexOutOfBoundsException


object Args extends Atributos {

  val logger: Logger = LogManager.getLogger("ETL - Data Pipeline - Test")
  var target=""

  def getArguments(args: Array[String]): Boolean = {
    println("METODO GETARGUMENTS")
    try {
      println(args.length)
      pathFile = args(0)
      datePart = args(1)
      tickerList = args(2).split(",").map(_.trim).toList
      numDay = args(3).toInt
      processName = args(4)
      modeExecution = args(5)
      target=args(6)


      if (args.length < 7) {
        logger.info("Check number of params, required 6 arguments")
        println("Check number of params, required 6 arguments")


        checkParamsBoolean
      }
      else {
        println("pathFile :" + pathFile)
        println("datePart :" + datePart)
        println("tickerList :" + tickerList)
        println("NumDay :" + numDay)
        println("processName :" + processName)
        println("ModeExecution :" + modeExecution)
        println("Target :" + target)

        logger.info("Parameters received")
        logger.info("Path: " + pathFile + " Date: " + datePart + " Ticker : " + tickerList + " NumDay: " + numDay)
        println("Path: " + pathFile + " Date: " + datePart + " Ticker : " + tickerList + " NumDay: " + numDay)
        checkParamsBoolean = true

        return checkParamsBoolean
      }
    }
    catch {
      case e: ArrayIndexOutOfBoundsException =>
        logger.info("required 7 parameters " + e)
        println("required 7  parameters " + e)
        checkParamsBoolean

    }
  }
}
