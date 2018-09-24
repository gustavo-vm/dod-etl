package com.ufmg.dodetl.imtu

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.util
import java.util.Properties

import com.ufmg.dodetl.config.Mysql
import com.ufmg.dodetl.utils.UtilDODETL
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema.Field

class DatabaseManager() extends Serializable {

  var connection: Connection = null

  def this(connection: Connection){
    this
    this.connection = connection
  }

  def this (mysqlConf: Mysql) {
    this
    val url = "jdbc:mysql://" + mysqlConf.host + ":" + mysqlConf.port.toInt + "/" + mysqlConf.db + "?user=" + mysqlConf.user + "&password=" + mysqlConf.passwd
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    this.connection = DriverManager.getConnection(url)

  }

  def createTableIfNotExist(tableName: String, schema: Schema) = {

    val query = getCreateTableQuery(tableName, schema)

    executeUpdateStatement(query)

  }

  def executeQueryStatement(query: String) = {
    val queryStatement = connection.createStatement()

    queryStatement.executeQuery(query)
  }

  private def executeUpdateStatement(query: String) = {
    val queryStatement = connection.createStatement()

    queryStatement.executeUpdate(query)

    connection.commit()
  }

  def getInsertPreparedStatement(schema: Schema, tableName: String) = {
    var prepState = "MERGE INTO " + tableName + " KEY(ID) VALUES ("

    schema.getFields.toArray.foreach { field =>
      prepState += "?"
      prepState = if (schema.getFields.toArray.last.asInstanceOf[Schema.Field] == field.asInstanceOf[Schema.Field]) prepState + " " else prepState + ", "
    }
    prepState += ") "

    connection.prepareStatement(prepState)
  }

  def valuesToPreparedStatement(genericRecord: GenericRecord, prepState: PreparedStatement) = {

    var index = 1

    val fields = genericRecord.getSchema.getFields.toArray.foreach { field =>

      val fieldCast = field.asInstanceOf[Schema.Field]
      val isTimestamp = checkLogicalType(fieldCast, "timestamp-millis")
      val isString = checkType(fieldCast, "string")
      if (isTimestamp) {
        prepState.setTimestamp(index, new Timestamp(genericRecord.get(fieldCast.name()).asInstanceOf[Long]))
      } else if (isString) {
        prepState.setObject(index, genericRecord.get(fieldCast.name()).toString)
      } else {
        prepState.setObject(index, genericRecord.get(fieldCast.name()))
      }
      index += 1

    }

    prepState.addBatch()
  }

  private def checkLogicalType(fieldCast: Field, fieldType: String) = {
    fieldCast.schema().getTypes.toArray.filter { x =>
      val lt = x.asInstanceOf[Schema].getLogicalType
      lt != null && lt.getName.contains(fieldType)
    }.length > 0
  }

  private def checkType(fieldCast: Field, fieldType: String) = {
    fieldCast.schema().getTypes.toArray.filter { x =>
      x.asInstanceOf[Schema].getName.contains(fieldType)
    }.length > 0
  }


  def getCreateTableQuery(tableName: String, schema: Schema) = {

    val indexFields = schema.getFields.toArray.filter(field => field.asInstanceOf[Schema.Field].doc().contains("INDEX"))


    var query = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
    schema.getFields.toArray().foreach { field =>

      query += field.asInstanceOf[Schema.Field].name() + " " + getODBCType(field.asInstanceOf[Schema.Field].schema(), field.asInstanceOf[Schema.Field].doc())
      query = if (schema.getFields.toArray().last.asInstanceOf[Schema.Field] == field) query + " " else query + ", "

    }
    query += "); "

    var count = 1
    indexFields.foreach { index =>
      val indexField = index.asInstanceOf[Schema.Field]
      val isPrimaryKey = indexField.doc().contains("PRIMARY KEY")
      val indexType = if (isPrimaryKey) "PRIMARY KEY" else "INDEX"
      val indexName = tableName + "_" + indexField.name() + "_" + String.valueOf(count)
      query += "CREATE " + indexType + " IF NOT EXISTS " + indexName + " ON " + tableName + "(" + indexField.name() + ");"
      count = count + 1
    }

    query
  }

  def getODBCType(schema: Schema, doc: String) = {

    val typeList = schema.getTypes.toArray.map(a => a.asInstanceOf[Schema])
    val schemaTypeNotNull = typeList.filter(typeName => !typeName.getName.contains("null")).head
    val logicalType = schemaTypeNotNull.getLogicalType()

    val docMap = UtilDODETL.jsonToMap(doc)

    var typeQuery = ""
    if (typeList.map(x => x.getName).contains("string")) {
      typeQuery += "VARCHAR(" + docMap.get("VARCHAR").get + ")"
    }
    else if (logicalType != null && logicalType.getName().contains("timestamp-millis")) {
      typeQuery += "TIMESTAMP"
    }
    else {
      typeQuery += schemaTypeNotNull.getName
    }

    typeQuery = if (typeList.map(x => x.getName).contains("null")) typeQuery + " NULL " else typeQuery + " NOT NULL "

    typeQuery

  }

  def dropTable(tableName: String) = {

    val query = "DROP TABLE IF EXISTS " + tableName

    executeUpdateStatement(query)

  }

  def deleteTableElementsByColVal(tableName: String, filterColumn: String, keysToDelete: util.HashSet[String]) = {

    val query = "DELETE FROM " + tableName + " WHERE " + filterColumn + " IN (" + String.join(",", keysToDelete) + ") "

    executeUpdateStatement(query)

  }

  def executeBatch(ps: PreparedStatement) = {

    ps.executeBatch()
    connection.commit()
  }

}
