package com.cms.challenge.etl

import com.cms.challenge.common.{Atributos, Reader, Writer}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{AnalysisException, Column, SparkSession}

class EtlTableCargo() extends Atributos {
  val reader = new Reader()
  val write = new Writer()

  val logger: Logger = LogManager
    .getLogger("ETL - Data Pipeline - Test")

  /**
   *
   * @param CSVFilePath
   * @param spark
   */



  def transformFieldAlertText(CSVFilePath: String)
                             (implicit spark: SparkSession): Unit = {
    try {
      val dfCargo = reader.readCSVFile(CSVFilePath)
        .select("*")
        .dropDuplicates()

      // Imprimir el schema
      val dfSelect = dfCargo.printSchema()
      // Mirar una pequena muestra de los datos..
      dfCargo.show(20, false)


      //Guardar los datos en formato parquet
      //write.saveResultSetAsParquet(dfCargo, saveModeOverwrite, formatParquet, companyName, createdAt, pathToSaveResultFile)
      //Guardar los datos en la bd de postgres
      //write.saveResultJDBC(dfCargo, saveModeAppend, db.urlDB, db.user, db.password, db.dbTableToWrite)
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
    }
  }

  def transformTableCargo(CSVFilePath: String)
                         (implicit spark: SparkSession): Unit = {
    try {
      val dfCargo = reader.readCSVFile(CSVFilePath)
        .select(col(id), col(name).as(companyName), col(companyId).as("company_id"),
          when(col(amount).isNotNull, col(amount).cast(DecimalType(16, 2))).as(amount)
          , col(status), transformDateField(createdAt).cast("timestamp"),
          col(paidAt).as(updateAt))
        .filter(trim(col(id)) =!= "")
        .filter(trim(col(amount)).rlike("[^0-9]"))
        .filter(trim(col(companyId)) =!= "")
        .filter(trim(col(companyId)) =!= "*******")
        .filter(trim(col(companyName)) =!= "")
        .filter(trim(col(createdAt)) =!= "")
        .filter(trim(col(status)) =!= "")
        .dropDuplicates()

      // Imprimir el schema
      val dfSelect = dfCargo.printSchema()
      // Mirar una pequena muestra de los datos..
      dfCargo.show(20, false)

     // db.getDBConnection()

      //Guardar los datos en formato parquet
      write.saveResultSetAsParquet(dfCargo, saveModeOverwrite, formatParquet, companyName, createdAt, pathToSaveResultFile)
      //Guardar los datos en la bd de postgres
      //write.saveResultJDBC(dfCargo, saveModeAppend, db.urlDB, db.user, db.password, db.dbTableToWrite)
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
    }
  }

  /**
   *
   * @param DateField Nombre de la columna que contiene el dato de fecha en string.
   * @return Column
   */
  def transformDateField(DateField: String): Column = {
    when(to_date(col(DateField), "yyyy-MM-dd").isNotNull,
      to_date(col(DateField), "yyyy-MM-dd"))
      .when(to_date(col(DateField), "yyyyMMdd").isNotNull,
        date_format(to_date(col(DateField), "yyyyMMdd"), "yyyy-MM-dd"))
      .when(to_date(col(DateField), "MM/dd/yyyy").isNotNull,
        to_date(col(DateField), "MM/dd/yyyy"))
      .when(to_date(col(DateField), "yyyy MMMM dd").isNotNull,
        to_date(col(DateField), "yyyy MMMM dd"))
      .when(to_date(col(DateField), "yyyy MMMM dd E").isNotNull,
        to_date(col(DateField), "yyyy MMMM dd E"))
      .otherwise("Unknown Format")
      .alias(DateField)
  }
}