package com.cms.challenge.common

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame}

class Writer extends Atributos {
  val formatDateComplet = "dd/MM/yyyy HH:mm"
  val dateFormat = new SimpleDateFormat(formatDateComplet)
  val driver = "org.postgresql.Driver"
  val logger: Logger = LogManager
    .getLogger("ETL - Data Pipeline - Test")

  /**
   *
   * @param df
   * @param saveMode
   * @param format
   * @param companyId
   * @param createdAt
   * @param pathToSave
   */
  def saveResultSetAsParquet(df: DataFrame, saveMode: String, format: String, companyId: String,
                             createdAt: String, pathToSave: String): Unit = {
    try {
      df.write
        .mode(saveMode)
        .partitionBy(createdAt, companyId)
        .option("compression", "snappy")
        .format(format)
        .save(pathToSave)
      logger.info("The Data save into the path : ".concat(pathToSave))
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check table about $ex ")
    }
  }

  /**
   *
   * @param df
   * @param saveOption
   * @param jdbcUrl
   * @param jdbcUser
   * @param jdbcPassword
   * @param tableToWrite
   */
  def saveResultJDBC(df: DataFrame, saveOption: String, jdbcUrl: String,
                     jdbcUser: String, jdbcPassword: String,
                     tableToWrite: String): Unit = {
    val connectionProperties = new Properties
    connectionProperties.put("user", jdbcUser)
    connectionProperties.put("password", jdbcPassword)
    try {
      df.write
        .mode(saveOption)
        .jdbc(jdbcUrl, tableToWrite, connectionProperties)
    }
    catch {
      case ex: IOException =>
        logger.error(s"Check the path about $ex ")
    }
  }
}

