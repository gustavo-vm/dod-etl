package com.ufmg.dodetl.imtu

import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.{ConcurrentHashMap, Phaser}

import com.ufmg.dodetl.config.TableConfig
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.h2.jdbcx.JdbcConnectionPool

import scala.collection.immutable.Map
import scala.collection.mutable.{ListBuffer}

class InMemoryTableUpdaterManager(bootStrapServer: String, schemaRegistryURL: String, groupID: String, metaTableConfigList: List[TableConfig]) extends Serializable{

  var lastTableTimeStamp = new ConcurrentHashMap[String, Long]
  var missingDataList = new ListBuffer[(Map[String, Object], List[String])]()

  val dataSource: JdbcConnectionPool = JdbcConnectionPool.create("jdbc:h2:mem:MES_DB;DB_CLOSE_DELAY=-1", "sa", "")
  val coreDBManager = new DatabaseManager(dataSource.getConnection)
  var filterKeySet = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean])
  var countDown: Phaser = new Phaser(metaTableConfigList.length + 1)
  var startKafka = starKafkaConsumer()


  def starKafkaConsumer(): Boolean = {

    val consumerCOnfig = getConsumerConfig(bootStrapServer, schemaRegistryURL, groupID)

    metaTableConfigList.foreach{metaTableConfig =>

      val DBManager = new DatabaseManager(dataSource.getConnection)
      val inMemTabUp = new InMemoryTableUpdater(DBManager, countDown, consumerCOnfig, filterKeySet, lastTableTimeStamp, metaTableConfig)
      new Thread(inMemTabUp).start()

    }

    true
  }

  private def getConsumerConfig(bootStrapServer: String, schemaRegistryURL: String, groupID: String) = {
    var config = new Properties()
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer)
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL)
    config.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName())
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName())
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    config
  }

  def updateFilterKeySet(filterKey: String): Unit ={
    //TODO: Remove key
    if(filterKey != null && !filterKeySet.contains(filterKey)){
      filterKeySet.add(filterKey)
      countDown.arriveAndAwaitAdvance()
    }else if(filterKey == null){
      filterKeySet.add("NOKEY")
      countDown.arriveAndAwaitAdvance()
    }

  }

  def checkForArrivedMissingData(): Unit = {

    missingDataList = missingDataList.filter{ tuple =>
      val ts = tuple._1.get("timestamp").asInstanceOf[Long]
      tuple._2.filter(table => lastTableTimeStamp.get(table) > ts).length == tuple._2.length
    }

  }

  def getMissingData() ={
    missingDataList.map(tuple => tuple._1)
  }

  def start(): Boolean = {startKafka}

  def getDBManager() = {
    coreDBManager
  }


}
