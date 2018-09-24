package com.ufmg.dodetl.utils

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import org.apache.spark.sql.{ForeachWriter, Row}

class JDBCSink(url: String) extends ForeachWriter[Double] {

  var connection: Connection = _
  var statement: Statement = _

  override def open(partitionId: Long, version: Long): Boolean = {
    connection = DriverManager.getConnection(url)
    statement = connection.createStatement()
    true
  }

  override def process(value: Double): Unit = {

    val newValue = if(value.isNaN) 0.0 else value

    val v_TableName = "OEE"
    val v_Query = "INSERT INTO OEE VALUES ( "+ newValue +", NOW(3))"
    statement.executeUpdate(v_Query)

  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }
}
