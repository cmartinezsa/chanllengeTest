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

  def getDBConnection()(implicit sparkSession: SparkSession) {
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    port = getPropertie("spark.service.user.postgresql.port")
    host = getPropertie("spark.service.user.postgresql.host")
    user = getPropertie("spark.service.user.postgresql.user")
    password = getPropertie("spark.service.user.postgresql.pass")
    database = getPropertie("spark.service.user.postgresql.database")
    urlDB = s"jdbc:postgresql://$host:$port/$database"
    urlSimple = s"jdbc:postgresql://$host:$port/"
    dbTableToWrite = database.concat(".").concat(Args.target)
  }

  private def getPropertie(propertie: String)(implicit sparkSession: SparkSession): String =
    sparkSession.sparkContext.getConf.get(propertie)
}
