package com.ufmg.dodetl.stp

import java.util.UUID

import com.ufmg.dodetl.config.{Kafka, Mysql, Spark, TableConfig}
import com.ufmg.dodetl.imtu.{DatabaseManager, InMemoryTableUpdaterManager}
import com.ufmg.dodetl.utils.{JDBCSink, SharedSingleton, UtilDODETL}
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.KV

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class BeamStreamProcessor(sparkConf: Spark, kafkaConf: Kafka, mysqlConf: Mysql,
                          operationTableConfig: TableConfig, metaTableConfigList: List[TableConfig]) extends Serializable {

  //TODO Implement window
  val WATERMARK_WINDOW_IN_MINUTES = 10
  val IMTUManager: SharedSingleton[InMemoryTableUpdaterManager] = if (sparkConf.memTable) SharedSingleton {
    val groupID = "IMTU-" + UUID.randomUUID().toString
    val IMTUManager = new InMemoryTableUpdaterManager(kafkaConf.bootstrapServers, kafkaConf.schemaRegistryUrl, groupID, metaTableConfigList)
    IMTUManager
  } else null
  val noMemDBManager: SharedSingleton[DatabaseManager] = if (sparkConf.memTable) null else SharedSingleton {
    new DatabaseManager(mysqlConf)
  }
  val writer = new JDBCSink("jdbc:mysql://" + mysqlConf.host + ":" + mysqlConf.port.toInt + "/" + mysqlConf.db + "?user=" + mysqlConf.user + "&password=" + mysqlConf.passwd)

  def process() = {

    val pipelineOptions = PipelineOptionsFactory.create
    pipelineOptions.setRunner(classOf[DirectRunner])
    val operationalPipeline = Pipeline.create(pipelineOptions)

    val kafkaReader = KafkaIO.read[String, String]
      .withBootstrapServers(kafkaConf.bootstrapServers)
      .withTopic(operationTableConfig.opTopicName)
      .withKeyDeserializer(classOf[StringDeserializer])
      .withValueDeserializer(classOf[StringDeserializer])


    operationalPipeline.apply(kafkaReader.withoutMetadata())
      .apply(Values.create[String]())
      .apply(MapElements.via(
        new SimpleFunction[String, ListBuffer[Map[String, Object]]] {
          override def apply(value: String): ListBuffer[Map[String, Object]] = {
            var productList = new ListBuffer[Map[String, Object]]()
            if (sparkConf.memTable) {
              val IMTUManagerInit = IMTUManager.get
              IMTUManagerInit.updateFilterKeySet(null) //        IMTUManagerInit.updateFilterKeySet(key)
              IMTUManagerInit.checkForArrivedMissingData()
              var productList = IMTUManagerInit.getMissingData()
            }
            productList += UtilDODETL.jsonToMap(value)

            productList
          }
        })).apply(MapElements.via(
      new SimpleFunction[ListBuffer[Map[String, Object]], mutable.Seq[Map[String, Object]]] {
        override def apply(productMapList: ListBuffer[Map[String, Object]]): mutable.Seq[Map[String, Object]] = {
          var factGrainList = new ListBuffer[Map[String, Object]]()
          for (productMap <- productMapList) {
            val dataTypeList = List("POPQ", "ES", "PS")
            val dbManager = if (sparkConf.memTable) IMTUManager.get.getDBManager() else noMemDBManager.get
            var anyPOPQDataMissing = false
            var anyESDataMissing = false

            var prodWithMeta: Map[String, Object] = null
            var ESList: ListBuffer[Map[String, Object]] = null
            var PSList: Map[String, Object] = null

            dataTypeList.par.foreach { dataType =>
              if (dataType == "POPQ") {
                prodWithMeta = DataTransformer.getProductRelatedMaterData(dbManager, productMap)
                anyPOPQDataMissing = prodWithMeta.get("ID_PRODUCTION_ORDER") == null
              } else if (dataType == "ES") {
                ESList = DataTransformer.getES(dbManager, productMap)
                anyESDataMissing = ESList.length == 0 || (ESList.last.get("END_TIME").get.asInstanceOf[java.sql.Timestamp]).before(new java.sql.Timestamp(productMap.get("END_TIME").get.asInstanceOf[BigInt].toLong))
              } //TODO: PeriodShift
            }

            var prodWithMasterDate: (Map[String, Any], ListBuffer[Map[String, Object]]) = (Map(), ListBuffer())

            if (anyPOPQDataMissing && anyESDataMissing) {
              prodWithMasterDate = (productMap, ESList)
            }

            if (anyPOPQDataMissing || anyESDataMissing) {
              //TODO: update missing product list
            }
            val partialFactGrainList: mutable.Seq[Map[String, Object]] = DataTransformer.splitBasedOnEquipStatus(prodWithMeta, ESList)
            factGrainList ++= partialFactGrainList
          }

          factGrainList
        }
      })).apply(MapElements.via(
      new SimpleFunction[mutable.Seq[Map[String, Object]], String] {
        override def apply(factGrainList: mutable.Seq[Map[String, Object]]): String = {
          var oeeList = new ListBuffer[Double]()
          for (factGrain <- factGrainList) {
            val factGrainWithOEE = DataTransformer.calculateOEE(factGrain)
            //      factGrainWithOEE
            val a: Double = factGrainWithOEE.get("OEE").get.asInstanceOf[Double]
            oeeList += a
          }
          oeeList.mkString(",")
        }
      }))
      .apply(TextIO.write().to("oee"));

    operationalPipeline.run()
  }

}

class FormatResult extends SimpleFunction[String, List[String]] {
  override def apply(input: String): List[String] = {
    List("", "")
  }
}