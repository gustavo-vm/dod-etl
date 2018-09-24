package com.ufmg.dodetl.stp


import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.ufmg.dodetl.imtu.DatabaseManager
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DataTransformer extends Serializable {

  val m_DateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def getProductRelatedMaterData(DBManager: DatabaseManager, productMap: Map[String, Object]) = {

    val idProd = productMap.get("ID").get
    val idPO = productMap.get("ID_PRODUCTION_ORDER").get
    val codEq = productMap.get("COD_EQUIPMENT").get
    val startTime = new Timestamp(productMap.get("START_TIME").get.asInstanceOf[BigInt].toLong)
    val endTime = new Timestamp(productMap.get("END_TIME").get.asInstanceOf[BigInt].toLong)
    val quant = productMap.get("QUANTITY").get

    //TODO: Get query from config file
    var query = "SELECT * FROM PRODUCTION_ORDER PO LEFT JOIN PRODUCT_QUALITY QUAL ON (PO.ID = " + idPO.asInstanceOf[BigInt] + " AND QUAL.ID_PRODUCT = " + idProd.asInstanceOf[BigInt] + ") WHERE PO.ID = 1;"

    val rs = DBManager.executeQueryStatement(query)
    rs.next()

    val map: Map[String, Object] = Map("ID" -> idProd, "ID_PRODUCTION_ORDER" -> idPO, "ID_MATERIAL" -> rs.getObject("ID_MATERIAL"),
      "COD_EQUIPMENT" -> codEq, "PLANNED_START_TIME" -> rs.getObject("PLANNED_START_TIME"), "PLANNED_END_TIME" -> rs.getTimestamp("PLANNED_END_TIME"),
      "START_TIME" -> startTime, "END_TIME" -> endTime, "PLANNED_QUANTITY" -> rs.getObject("PLANNED_QUANTITY"), "QUANTITY" -> quant,
      "IS_OUT_OF_SPECIFICATION" -> rs.getObject("IS_OUT_OF_SPECIFICATION"))

    map
  }

  def getES(DBManager: DatabaseManager, productMap: Map[String, Object]) = {

    val startTime = new Timestamp(productMap.get("START_TIME").get.asInstanceOf[BigInt].toLong)
    val endTime = new Timestamp(productMap.get("END_TIME").get.asInstanceOf[BigInt].toLong)
    val codEq = productMap.get("COD_EQUIPMENT").get.asInstanceOf[String]

    var query = "SELECT * FROM EQUIPMENT_STATUS WHERE START_TIME < '" + m_DateFormat.format(endTime) + "' AND END_TIME > '" + m_DateFormat.format(startTime) + "' AND COD_EQUIPMENT = '" + codEq + "' "

    var rs = DBManager.executeQueryStatement(query)

    var ESList = new ListBuffer[Map[String, Object]]

    while (rs.next()) {

      ESList += Map("ID" -> rs.getObject("ID"), "COD_EQUIPMENT" -> codEq, "START_TIME" -> rs.getObject("START_TIME"),
        "END_TIME" -> rs.getObject("END_TIME"), "STATUS_TYPE" -> rs.getString("STATUS_TYPE"),
        "IS_FREE_TIME" -> rs.getObject("IS_FREE_TIME"))

    }

    ESList
  }

  def splitBasedOnEquipStatus(p: Map[String, Object], esl: ListBuffer[Map[String, Object]]): ListBuffer[Map[String, Object]] = {

    val prodStartTime = p.get("START_TIME").get.asInstanceOf[Timestamp]
    val prodEndTime = p.get("END_TIME").get.asInstanceOf[Timestamp]


    var partialFactGrainList = new ListBuffer[Map[String, Object]]()

    esl.foreach { es =>

      val esStartDate = es.get("START_TIME").get.asInstanceOf[Timestamp]
      val esEndDate = es.get("END_TIME").get.asInstanceOf[Timestamp]

      var partialFactStartDate: Timestamp = null
      var partialFactEndDate: Timestamp = null

      if (prodStartTime.after(esStartDate))
        partialFactStartDate = prodStartTime
      else
        partialFactStartDate = esStartDate

      if (prodEndTime.before(esEndDate))
        partialFactEndDate = prodEndTime
      else
        partialFactEndDate = esEndDate


      val factMap = Map("ID" -> p.get("ID").get, "ID_PRODUCTION_ORDER" -> p.get("ID_PRODUCTION_ORDER").get, "ID_MATERIAL" -> p.get("ID_MATERIAL").get, "ID_EQUIPMENT_STATUS" -> es.get("ID").get,
        "COD_EQUIPMENT" -> p.get("COD_EQUIPMENT").get, "PLANNED_START_TIME" -> p.get("PLANNED_START_TIME").get, "PLANNED_END_TIME" -> p.get("PLANNED_END_TIME").get,
        "FACT_START_TIME" -> partialFactStartDate, "FACT_END_TIME" -> partialFactEndDate, "PROD_START_TIME" -> prodStartTime, "PROD_END_TIME" -> prodEndTime,
        "PLANNED_QUANTITY" -> p.get("PLANNED_QUANTITY").get, "QUANTITY" -> p.get("QUANTITY").get, "IS_OUT_OF_SPECIFICATION" -> p.get("IS_OUT_OF_SPECIFICATION").get,
        "STATUS_TYPE" -> es.get("STATUS_TYPE").get, "IS_FREE_TIME" -> es.get("IS_FREE_TIME").get)

      partialFactGrainList += factMap
    }

    partialFactGrainList
  }

  def calculateOEE(factGrain: Map[String, Any]): Map[String, Any] = {

    val factStartTime = factGrain.get("FACT_START_TIME").get.asInstanceOf[Timestamp]
    val factEndTime = factGrain.get("FACT_END_TIME").get.asInstanceOf[Timestamp]
    val factDuration = (factEndTime.getTime - factStartTime.getTime).toDouble

    val prodStartTime = factGrain.get("PROD_START_TIME").get.asInstanceOf[Timestamp]
    val prodEndTime = factGrain.get("PROD_END_TIME").get.asInstanceOf[Timestamp]
    val prodDuration = (prodEndTime.getTime - prodStartTime.getTime).toDouble

    val plannedStartTime = factGrain.get("PLANNED_START_TIME").get.asInstanceOf[Timestamp]
    val plannedEndTime = factGrain.get("PLANNED_END_TIME").get.asInstanceOf[Timestamp]
    val plannedDuration = (plannedEndTime.getTime - plannedStartTime.getTime).toDouble

    val isStatusOff =  factGrain.get("STATUS_TYPE").get.asInstanceOf[String].contains("OFF")

    val isOutOfSpecification = factGrain.get("IS_OUT_OF_SPECIFICATION").get.asInstanceOf[Boolean]

    val splitProportionalFactor = if(isStatusOff) 0.0 else factDuration/prodDuration

    val productProportionalFactor = if(isStatusOff) 0.0 else prodDuration/plannedDuration

    val quantity = factGrain.get("QUANTITY").get.asInstanceOf[Double]

    val plannedQuantity = factGrain.get("PLANNED_QUANTITY").get.asInstanceOf[Double]

    val performance = splitProportionalFactor * (quantity / (productProportionalFactor * plannedQuantity))

    val availability = if(isStatusOff) 0.0 else 1.0

    val quality = if(isOutOfSpecification) 0.0 else 1.0

    val oee = performance * availability * quality


    val factGrainWithOEE = Map("ID" -> factGrain.get("ID_PRODUCT"), "ID_PRODUCTION_ORDER" -> factGrain.get("ID_PRODUCTION_ORDER"),
      "ID_MATERIAL" -> factGrain.get("ID_MATERIAL"), "ID_EQUIPMENT_STATUS" -> factGrain.get("ID"), "COD_EQUIPMENT" -> factGrain.get("COD_EQUIPMENT"),
    "PERFORMANCE" -> performance, "AVAILABILITY" -> availability, "QUALITY" -> quality, "OEE" -> oee)

    factGrainWithOEE
  }


}
