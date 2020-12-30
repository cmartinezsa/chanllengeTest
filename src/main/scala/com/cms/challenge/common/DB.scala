package com.cms.challenge.common

import org.apache.spark.sql.SparkSession

class DB extends Atributos {
  var port = ""
  var user = ""
  var password = ""
  var host = ""
  var database = ""
  var urlDB = ""
  var urlSimple = ""
  var dbTableToWrite = ""

  /**
    *
    * @param sparkSession
    */

  def getDBConnection()(implicit sparkSession: SparkSession) {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    port = getPropertie(postgresPort)
    host = getPropertie(postgresHost)
    user = getPropertie(postgresUser)
    password = getPropertie(postgresPassword)
    database = getPropertie(postgresDatabase)
    urlDB = s"jdbc:postgresql://$host:$port/$database"
    urlSimple = s"jdbc:postgresql://$host:$port/"
    dbTableToWrite = database.concat(".").concat(Args.target)
  }

  /**
    *
    * @param propertie
    * @param sparkSession
    * @return
    */
  private def getPropertie(propertie: String)
                          (implicit sparkSession: SparkSession): String =
    sparkSession.sparkContext.getConf.get(propertie)
}
