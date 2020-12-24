package com.cms.challenge.etl

import com.cms.challenge.common.Args.target
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.expressions.Window
import com.cms.challenge.common.{Args, Atributos, DB, Reader, Writer}

class StockPrices extends Atributos {
  private val reader = new Reader()
  private val logger: Logger = LogManager.getLogger("ETL - Data Pipeline - Test")
  val db = new DB()
  val write = new Writer()


  /**
    *
    * @param pathFile
    * @param date_part
    * @param ticker
    * @param days
    * @param spark
    */
  def stockPricesETL(pathFile: String, date_part: String, ticker: List[String], days: Int)
                    (implicit spark: SparkSession): DataFrame = {
    logger.info("Parameters recieved : " + pathFile + " " + date_part + " " + ticker + " " + days)

    val emptyDF = spark.emptyDataFrame
    try {
      val dfPrices = reader.readCSVFile(pathFile, date_part, ticker, days)

     val dfFinalMovilAvg= getJoinDFs(getMovAvgPrices(dfPrices))


      //val dfShow = dfFinalMovilAvg.show(1000, false)

      if (!dfFinalMovilAvg.isEmpty){
        db.getDBConnection()
        println("PARAMETROS DE LA BD ")
        println(db.urlDB)
        println(db.user)
        println(db.password)
        println(db.dbTableToWrite)
        println(saveModeAppend)
        println("Target :" + target)


        //Guardar los datos en formato parquet
        //write.saveResultSetAsParquet(movAvg, saveModeOverwrite, formatParquet, companyName, createdAt, pathToSaveResultFile)
        //Guardar los datos en la bd de postgres
        write.saveResultJDBC(dfFinalMovilAvg, saveModeAppend, db.urlDB, db.user, db.password, target)
        logger.info("**** WRITE SUCESFULL ****")

      }else{
        logger.info("DataFrame not contains data")
      }

      dfFinalMovilAvg

    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        emptyDF

    }
  }

  def getMovAvgPrices(df: DataFrame)(implicit sparkSession: SparkSession): List[DataFrame] = {

    val movAvgOpen = df.select(col(tickerField), col(dateField), col(openValueField))
      .withColumn("movingAverageOpen", avg(col(openValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))
      .persist()
    logger.info("Creado movAvgOpen ")



    val movAvgClose = df.select(col(tickerField), col(dateField), col(closeValueField))
      .withColumn("movingAverageClose", avg(col(closeValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))
      .persist()

    val movAvgAdjClose = df.select(col(tickerField), col(dateField), col(adjCloseValueField))
      .withColumn("movingAverageAdjClose", avg(col(adjCloseValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))
      .persist()

    val movAvgHigh = df.select(col(tickerField), col(dateField), col(highValueField))
      .withColumn("movingAverageHigh", avg(col(highValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))
      .persist()

    val movAvgVolume = df.select(col(tickerField), col(dateField), col(volumeValueField))
      .withColumn("movingAverageVolume", avg(col(volumeValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))
        .persist()

    logger.info("Creado movAvgOpen, movAvgClose, movAvgAdjClose, movAvgHigh, movAvgVolume ")
    List(movAvgOpen, movAvgClose, movAvgAdjClose, movAvgHigh, movAvgVolume)

  }

  def getJoinDFs(dfs: List[DataFrame])(implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame
    if (dfs.nonEmpty) {
      logger.info("DataFrame contains data")
      val dfFirst = dfs(0)
      val dfSecond = dfs(1)
      val dfThird = dfs(2)
      val dfFour = dfs(3)
      val dfFive = dfs(4)

      val dfJoinResultFirstPart = dfFirst.join(dfSecond, Seq(tickerField, dateField), innerJoin)
        .join(dfThird, Seq(tickerField, dateField), innerJoin)
        .persist()

    logger.info("Creado dfJoinResultFirstPart")
    /*  val dfJoinResultFirstPartShow=dfJoinResultFirstPart
      dfJoinResultFirstPartShow.show(100, false)

     */

      val dfJoinResultSecondPart = dfJoinResultFirstPart.join(dfFour, Seq(tickerField, dateField), innerJoin)
        .join(dfFive, Seq(tickerField, dateField), innerJoin)
          .persist()
/*
      val dfJoinResultSecondPartShow=dfJoinResultSecondPart
      dfJoinResultSecondPartShow.show(100, false)
      s
 */
      logger.info("Creado dfJoinResultSecondPart")

      dfJoinResultSecondPart


    } else {
      logger.info("Not found DFs to Join")
   dfEmpty
    }

  }

}
