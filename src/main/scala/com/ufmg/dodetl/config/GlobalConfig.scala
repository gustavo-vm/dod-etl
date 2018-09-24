package com.ufmg.dodetl.config

case class GlobalConfig(
                         spark: Spark,
                         kafka: Kafka,
                         mysql: Mysql,
                         numKafkaTables: Double
                       )

case class Spark(
                  jobName: String,
                  local: Boolean,
                  memTable: Boolean,
                  offset: String
                )

case class Kafka(
                  bootstrapServers: String,
                  schemaRegistryUrl: String
                )

case class Mysql(
                  host: String,
                  port: Double,
                  db: String,
                  user: String,
                  passwd: String
                )

