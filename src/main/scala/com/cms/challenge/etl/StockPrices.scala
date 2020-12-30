package com.cms.challenge.etl

import com.cms.challenge.common.Args.target
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.expressions.Window
import com.cms.challenge.common.{Atributos, DB, Reader, Writer}

class StockPrices extends Atributos {
  val pathToSaveResultTemp = "/Users/yaakov.chaym/Documents/dataset/challengede/out"
  private val reader = new Reader()
  private val logger: Logger = LogManager.getLogger("ETL - Data Pipeline - Challenge")
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

      if (!dfPrices.isEmpty) {
        logger.info("The dataframe contains data.")
        write.saveResultSetAsParquet(dfPrices, saveModeOverwrite, formatParquet, tickerField, dateField, pathToSaveResultTemp)

      } else {
        logger.info("The dataframe not contains data.")
      }
      val dfPriceSelected = reader.readParquetFile(pathToSaveResultTemp)

      println("SCHEMA PARQUET FILE")
      val printSchemaDF = dfPriceSelected
      printSchemaDF.printSchema()
      val countReg = printSchemaDF.count()
      print("CANTIDAD DE REGISTROS"+countReg)


      val dfFinalMovilAvg = getJoinDFs(getMovAvgPrices(dfPriceSelected))
      if (!dfFinalMovilAvg.isEmpty) {
        db.getDBConnection()
        //Guardar los datos en formato parquet
        //write.saveResultSetAsParquet(dfFinalMovilAvg, saveModeOverwrite, formatParquet, tickerField, dateField, target)
        //Guardar los datos en la bd de postgres
        write.saveResultJDBC(dfFinalMovilAvg, saveModeAppend, db.urlDB, db.user, db.password, target)
        logger.info(s"**** Write Sucessfull into $target****")

      } else {
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

  /**
    *
    * @param df
    * @param sparkSession
    * @return Five DataFrames with Column of moving average to key indicators.
    */
  def getMovAvgPrices(df: DataFrame)(implicit sparkSession: SparkSession): List[DataFrame] = {

    val movAvgOpen = df.select(col(tickerField), col(dateField), col(openValueField))
      .withColumn("movingAverageOpen", avg(col(openValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    val movAvgClose = df.select(col(tickerField), col(dateField), col(closeValueField))
      .withColumn("movingAverageClose", avg(col(closeValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    val movAvgAdjClose = df.select(col(tickerField), col(dateField), col(adjCloseValueField))
      .withColumn("movingAverageAdjClose", avg(col(adjCloseValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    val movAvgHigh = df.select(col(tickerField), col(dateField), col(highValueField))
      .withColumn("movingAverageHigh", avg(col(highValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    val movAvgLow = df.select(col(tickerField), col(dateField), col(lowValueField))
      .withColumn("movingAverageLow", avg(col(lowValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    val movAvgVolume = df.select(col(tickerField), col(dateField), col(volumeValueField))
      .withColumn("movingAverageVolume", avg(col(volumeValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    logger.info("Creado movAvgOpen, movAvgClose, movAvgAdjClose, movAvgHigh, movAvgVolume ")

    List(movAvgOpen, movAvgClose, movAvgAdjClose, movAvgHigh, movAvgLow, movAvgVolume)

  }

  /**
    *
    * @param dfs
    * @param spark
    * @return DataFrame with all join.
    */

  def getJoinDFs(dfs: List[DataFrame])(implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame

    if (dfs.nonEmpty) {
      logger.info("DataFrame contains data")
      val dfFirst = dfs(0)
      val dfSecond = dfs(1)
      val dfThird = dfs(2)
      val dfFour = dfs(3)
      val dfFive = dfs(4)
      val dfSix = dfs(5)
      val dfJoinResultFirstPart = dfFirst.join(dfSecond, Seq(tickerField, dateField), innerJoin)
        .join(dfThird, Seq(tickerField, dateField), innerJoin)

      logger.info("Created dfJoinResultFirstPart")

      val dfJoinResultSecondPart = dfJoinResultFirstPart.join(dfFour, Seq(tickerField, dateField), innerJoin)
        .join(dfFive, Seq(tickerField, dateField), innerJoin)
        .join(dfSix, Seq(tickerField, dateField), innerJoin)
        .persist()

      logger.info("Created dfJoinResultSecondPart")

      dfJoinResultSecondPart

    } else {
      logger.info("Not found DFs to Join")
      dfEmpty
    }
  }
}
