package com.ufmg.dodetl.config

case class TableConfig(tableName: String, metaTopicName: String, opTopicName: String, filterColumn: String, transacTimestampCol: String, isMetadata: Boolean,
                       isOperational: Boolean, retentionPeriodInDays: Int) {

}
