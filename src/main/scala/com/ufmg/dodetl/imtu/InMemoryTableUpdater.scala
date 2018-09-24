package com.ufmg.dodetl.imtu

import java.sql.{Connection, PreparedStatement}
import java.{lang, util}
import java.util.{Calendar, Properties, UUID}

import collection.JavaConversions._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, CyclicBarrier, Phaser}
import java.util.concurrent.atomic.AtomicBoolean

import com.ufmg.dodetl.config.TableConfig
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import scala.collection.mutable

class InMemoryTableUpdater() extends Runnable {

  var databaseManager: DatabaseManager = null
  var countDownLatch: Phaser = null
  var globalFilterKeySet: java.util.Set[String] = null
  var localFilterKeySet: java.util.Set[String] = new java.util.HashSet[String]()
  var lastTableTimestamp: ConcurrentHashMap[String, Long] = null

  var consumer: KafkaConsumer[GenericRecord, GenericRecord] = null
  var consumerConf: Properties = null
  var tableConfig: TableConfig = null
  var isShutDown: AtomicBoolean = null
  var shutdownLatch: CountDownLatch = null

  def this(databaseManager: DatabaseManager, countDownLatch: Phaser, consumerConf: Properties, filterKeySet: java.util.Set[String],
           lastTableTimestamp: ConcurrentHashMap[String, Long], tableConfig: TableConfig) {
    this
    this.databaseManager = databaseManager
    this.globalFilterKeySet = filterKeySet
    this.tableConfig = tableConfig
    this.consumerConf = consumerConf
    this.lastTableTimestamp = lastTableTimestamp
    this.isShutDown = new AtomicBoolean(false)
    this.shutdownLatch = new CountDownLatch(1)

    this.countDownLatch = countDownLatch

  }


  private def isDateAfterRetention(record: ConsumerRecord[GenericRecord, GenericRecord]) = {
    var currentRecordTime = record.value().get(tableConfig.transacTimestampCol).asInstanceOf[Timestamp]

    var retentionDate = Calendar.getInstance()
    retentionDate.add(Calendar.DAY_OF_MONTH, tableConfig.retentionPeriodInDays)

    val isAfter = true //currentRecordTime > retentionDate.getTimeInMillis
    isAfter
  }

  override def run(): Unit = {

    try {

      //      consumer.subscribe(Set(tableConfig.metaTopicName))
      while (!isShutDown.get()) {

        if (!globalFilterKeySet.isEmpty) {

          if (!globalFilterKeySet.equals(localFilterKeySet)) {
            println(tableConfig.tableName)
            dumpTopicAndRebuildTable(true)

          } else if (!globalFilterKeySet.isEmpty) {
            processRecordList(false, null)

            //TODO retention (table and keys) DELETE FROM positions WHERE time < TIMESTAMPADD('DAY', -7, NOW());

          }
        }
      }


    }
    finally {
      if (consumer != null)
        consumer.close()
      shutdownLatch.countDown()
    }
  }

  def cleanTable(localFilterKeySet: util.Set[String], globalFilterKeySet: util.Set[String]) = {
    var newKeys = new util.HashSet[String](globalFilterKeySet)
    newKeys.removeAll(localFilterKeySet)

//    if (newKeys.size() > 0) {
//      databaseManager.dropTable(tableConfig.tableName)
//    } else {
    if(newKeys.size() == 0){
      var keysToDelete = new util.HashSet[String](localFilterKeySet)
      keysToDelete.removeAll(globalFilterKeySet)
      databaseManager.deleteTableElementsByColVal(tableConfig.tableName, tableConfig.filterColumn, keysToDelete)
    }

  }


  def dumpTopicAndRebuildTable(firstExecution: Boolean) = {
    if (consumer != null) {
      consumer.unsubscribe()
      consumer.close()
    }
    consumer = new KafkaConsumer[GenericRecord, GenericRecord](consumerConf)
    //    consumer.subscribe(Set(tableConfig.metaTopicName))
    val partitions: Array[TopicPartition] = consumer.partitionsFor(tableConfig.metaTopicName).toArray.map(partInfo => new TopicPartition(partInfo.asInstanceOf[PartitionInfo].topic(), partInfo.asInstanceOf[PartitionInfo].partition()))
    this.consumer.assign(partitions.toSeq)
    this.consumer.seekToBeginning(partitions.toSeq)
    for (partition <- partitions) {
      var offset = consumer.position(partition)
      consumer.seek(partition, offset)
    }
    val endOffsets: util.Map[TopicPartition, lang.Long] = consumer.endOffsets(partitions.toSeq)

    cleanTable(localFilterKeySet, globalFilterKeySet)
    localFilterKeySet = new java.util.HashSet[String]()
    localFilterKeySet.addAll(globalFilterKeySet)
    processRecordList(firstExecution, endOffsets)
    countDownLatch.arriveAndDeregister()
  }

  def getLastValueFromEachKey(endOffsets: util.Map[TopicPartition, lang.Long]) = {

    var isEnd = false

    val keyOffsetMap = mutable.Map[String, Long]()
    val keyValueMap = mutable.Map[String, ConsumerRecord[GenericRecord, GenericRecord]]()
    val currentPartitionOffset = mutable.Map[Int, Long]()

    while(!isEnd) {
      val records = consumer.poll(1000)

      records.foreach { record =>

        var filterValue = record.value().get(tableConfig.filterColumn)
        val isRecordFromKey = filterValue == null || localFilterKeySet.contains(filterValue.toString)
        val noKey = localFilterKeySet.contains("NOKEY")

        if (noKey || isRecordFromKey && isDateAfterRetention(record)) {

          val offset = record.offset()
          val key = record.key().get("value").toString
          val value = record.value()

          if (!keyOffsetMap.contains(key) || keyOffsetMap.get(key).get < offset) {

            keyOffsetMap.put(key, offset)
            keyValueMap.put(key, record)

          }



        }
        currentPartitionOffset.put(record.partition(), record.offset())
      }

      isEnd = endOffsets.map(x => (currentPartitionOffset.contains(x._1.partition()) && currentPartitionOffset.get(x._1.partition()).get >= (x._2-1))|| x._2 == 0L).foldLeft(true)(_ && _)
    }


    keyValueMap.map(x => x._2)

  }

  private def processRecordList(firstExecution: Boolean, endOffsets: util.Map[TopicPartition, lang.Long]) = {
    var lastTimestamp = 0L
    var isFirstIteration = true

    var ps: PreparedStatement = null
    val records = if (firstExecution) getLastValueFromEachKey(endOffsets) else consumer.poll(500).toList

    records.foreach { record =>

      val filterValue = record.value().get(tableConfig.filterColumn)
      val isRecordFromKey = filterValue == null || localFilterKeySet.contains(filterValue.toString)
      val hasNoKey = localFilterKeySet.contains("NOKEY")

      val value = record.value()
      if (hasNoKey || isRecordFromKey && isDateAfterRetention(record)) {


        if (isFirstIteration && firstExecution) {
          databaseManager.createTableIfNotExist(tableConfig.tableName, value.getSchema)
        }

        if (isFirstIteration) {
          isFirstIteration = false
          ps = databaseManager.getInsertPreparedStatement(value.getSchema, tableConfig.tableName)
        }

        databaseManager.valuesToPreparedStatement(value, ps)

        val currentTS = record.value().get(tableConfig.transacTimestampCol).asInstanceOf[Timestamp]
        if (lastTimestamp < currentTS) {
          lastTimestamp = currentTS
        }

      }

    }

    if (ps != null) {
      databaseManager.executeBatch(ps)
      lastTableTimestamp.put(tableConfig.tableName, lastTimestamp)
    }

  }


  def shutdown() = {
    isShutDown.set(true)
    shutdownLatch.await()
  }
}
