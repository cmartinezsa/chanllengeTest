package com.cms.challenge.operator

import com.cms.challenge.common.Atributos
import com.cms.challenge.etl.EtlTableCargo
import com.cms.challenge.etl.StockPrices
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object ETLJobOperator extends  Atributos{
  def main(args: Array[String]): Unit = {
    val PATH = "/home/cms/IdeaProjects/TestSeccionDos/src/test/resources/*"
    val PATH_ALERT="/home/cms/Documentos/dataset/FileCSV/caso_norkom_salto2.csv"
    val PATH_STOCK="/home/cms/Documentos/dataset/hist/historical_stock_prices.csv"
    val ARG_DATE="2020-05-13"
    val ETLCARGO = new EtlTableCargo()
    val STOCK_PRICES=new StockPrices()

    val LOGGER: Logger = LogManager
      .getLogger(msjLog)

    LOGGER.info("Iniciando Proceso")
    val SPARK = SparkSession
      .builder()
      .config("spark.service.user.postgresql.pass", "cmsdb")
      .config("spark.service.user.postgresql.user", "cms")
      .config("spark.service.user.postgresql.database", "db_test_daen")
      .config("spark.service.user.postgresql.port", "5432")
      .config("spark.service.user.postgresql.host", "localhost")
      .master("local")
      .appName("ETL - Data Pipeline - Test")
      .getOrCreate()
    //ETLCARGO.transformTableCargo(PATH)(SPARK)
    //ETLCARGO.transformFieldAlertText(PATH_ALERT)(SPARK)
    STOCK_PRICES.stockPricesETL(PATH_STOCK, ARG_DATE)(SPARK)

  }
}
