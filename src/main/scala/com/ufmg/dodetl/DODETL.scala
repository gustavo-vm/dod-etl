package com.ufmg.dodetl


import com.ufmg.dodetl.config.ConfigLoader
import com.ufmg.dodetl.stp.BeamStreamProcessor
import scala.collection.JavaConverters._

object DODETL {


  def main(arg: Array[String]): Unit = {


    val configLoader = new ConfigLoader()
    val globalConfig = configLoader.loadGlobalConfig()
    val tableConfigurationList = configLoader.loadTableConfigList()

    val sparkLocal = globalConfig.spark.local
    val memTable = globalConfig.spark.memTable
    val offset = globalConfig.spark.offset
    val bootStrapServer = globalConfig.kafka.bootstrapServers
    val schemaRegistryURL = globalConfig.kafka.schemaRegistryUrl
    val operationTableConfig = tableConfigurationList.filter(x => x.isOperational).head
    val metaTableConfigList = tableConfigurationList.filter(x => x.isMetadata)


    val streamProcessor = new BeamStreamProcessor(globalConfig.spark, globalConfig.kafka, globalConfig.mysql, operationTableConfig, metaTableConfigList)
    streamProcessor.process()

  }


}
