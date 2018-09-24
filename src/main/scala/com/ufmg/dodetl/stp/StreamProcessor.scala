package com.ufmg.dodetl.stp

import java.util
import java.util.UUID

import com.ufmg.dodetl.config.{Kafka, Mysql, Spark, TableConfig}
import com.ufmg.dodetl.imtu.{DatabaseManager, InMemoryTableUpdaterManager}
import com.ufmg.dodetl.utils.{JDBCSink, SharedSingleton, UtilDODETL}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer

class StreamProcessor(sparkConf: Spark, kafkaConf: Kafka, mysqlConf: Mysql,
                      operationTableConfig: TableConfig, metaTableConfigList: List[TableConfig]) extends Serializable {

  //TODO Implement window
  val WATERMARK_WINDOW_IN_MINUTES = 10
  val sparkSession: SparkSession = startSparkSession()
  val IMTUManager: SharedSingleton[InMemoryTableUpdaterManager] = if (sparkConf.memTable) SharedSingleton {
    val groupID = "IMTU-" + UUID.randomUUID().toString
    val IMTUManager = new InMemoryTableUpdaterManager(kafkaConf.bootstrapServers, kafkaConf.schemaRegistryUrl, groupID, metaTableConfigList)
    IMTUManager
  } else null
  val noMemDBManager: SharedSingleton[DatabaseManager] = if (sparkConf.memTable) null else SharedSingleton {
    new DatabaseManager(mysqlConf)
  }
  val writer = new JDBCSink("jdbc:mysql://" + mysqlConf.host + ":" + mysqlConf.port.toInt + "/" + mysqlConf.db + "?user=" + mysqlConf.user + "&password=" + mysqlConf.passwd)

  private def startSparkSession(): SparkSession = {

    var ss: SparkSession = null
    if (sparkConf.local) {
      ss = SparkSession.builder()
        .master("local[*]")
        .appName(sparkConf.jobName).getOrCreate()
    }
    else {
      ss = SparkSession.builder()
        .appName(sparkConf.jobName).getOrCreate()
    }

    ss.conf.set("spark.network.timeout", "240")
    ss.conf.set("spark.scheduler.mode", "FAIR")
    ss.conf.set("spark.akka.frameSize", "200")
    ss.conf.set("spark.worker.cleanup.enabled", "true")


    //    Logger.getRootLogger().setLevel(Level.ERROR)

    ss
  }

  def initOperationalStreaming() = {


    val operationalReadStream = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConf.bootstrapServers)
      .option("startingOffsets", sparkConf.offset)
      .option("subscribe", operationTableConfig.opTopicName)
      .load()

    operationalReadStream
  }

  def process() = {


    val operationalReadStream = initOperationalStreaming()

    startStreamMetrics(sparkSession)

    import com.ufmg.dodetl.utils.DODETLEncoders._
    import sparkSession.implicits._

//    val IMTUManagerInit = IMTUManager.get
//    IMTUManagerInit.updateFilterKeySet(null)

    operationalReadStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)].flatMap { case (key, value) =>
      var productList = new ListBuffer[Map[String, Object]]()
      if (sparkConf.memTable) {
        val IMTUManagerInit = IMTUManager.get
        IMTUManagerInit.updateFilterKeySet(null)//        IMTUManagerInit.updateFilterKeySet(key)
        IMTUManagerInit.checkForArrivedMissingData()
        var productList = IMTUManagerInit.getMissingData()
      }
      productList += UtilDODETL.jsonToMap(value)

      productList
    }.flatMap { productMap =>

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
      val partialFactGrainList = DataTransformer.splitBasedOnEquipStatus(prodWithMeta, ESList)
      partialFactGrainList

    }.map { factGrain =>
      val factGrainWithOEE = DataTransformer.calculateOEE(factGrain)
      //      factGrainWithOEE
      factGrainWithOEE.get("OEE").get.asInstanceOf[Double]
    }
      .writeStream
      //      .foreach(writer)
      .format("memory")
      .queryName("oee")
      .start()
      .awaitTermination()
  }

  private def startStreamMetrics(session: SparkSession) = {
    session.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        if(queryProgress.progress.sources(0).numInputRows > 0) {
          val input = queryProgress.progress.sources(0).inputRowsPerSecond
          val output = queryProgress.progress.sources(0).processedRowsPerSecond
          println("inputRows (s): " + input + " / " + "processedRow (s): " + output)
        }
      }
    })
  }

}
