package com.cms.challenge.common

trait Atributos {
  val id = "id"
  val name = "name"
  val companyName = "company_name"
  val companyId = "company_id"
  val amount = "amount"
  val status = "status"
  val createdAt = "created_at"
  val paidAt = "paid_at"
  val updateAt = "updated_at"
  val pathToSaveResultFile = "/home/cms/Documentos/dataset/test_data_engineer1"
  val saveModeAppend = "append"
  val saveModeOverwrite = "overwrite"
  val formatParquet = "parquet"
  val compresionSnappy = "snappy"
  val msjLog = "ETL - Data Pipeline - Test -"
  val tickerField = "ticker"
  val openValueField = "open"
  val closeValueField = "close"
  val adjCloseValueField = "adj_close"
  val lowValueField = "low"
  val highValueField = "high"
  val volumeValueField = "volume"
  val dateField = "date"
  val dateFormatField = "date_formated"
  var pathFile = ""
  var datePart = ""
  var numDay=0
  var processName = ""
  var modeExecution = ""
  val formatDate = "yyyy-MM-dd"
  val castDate = "date"
  var checkParamsBoolean = false
  var tickerList = List(toString)
  var dateStartToExtract="start_date"
  val innerJoin="inner"

 //var target="hist_stock_prices"
}
