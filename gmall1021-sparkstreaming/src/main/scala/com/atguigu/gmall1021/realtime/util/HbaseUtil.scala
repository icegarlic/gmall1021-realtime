package com.atguigu.gmall1021.realtime.util

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

import java.util
import java.util.Properties

object HbaseUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")
  val DEFAULT_NAMESPACE = "GMALL1021"
  val DEFAULT_FAMILY = "INFO"
  val HBASE_SERVER = properties.getProperty("hbase.server")

  var connection: Connection = null

  def put(tableName: String, rowkey: String, dataJsonObj: JSONObject): Unit = {
    if (connection == null) init()
    // 从连接中获得表对象
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val putList: util.ArrayList[Put] = new util.ArrayList[Put]()
    // 组合多个put，每个put代表提交一个字段
    import collection.JavaConverters._
    for (entry <- dataJsonObj.entrySet().asScala) {
      val columnName: String = entry.getKey
      var columnValue: String = null
      if (entry.getValue!= null) {
        columnValue = entry.getValue.toString
      }
      // 创建put动作
      val put: Put = new Put(Bytes.toBytes(rowkey))
        .addColumn(Bytes.toBytes(DEFAULT_FAMILY), Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
      putList.add(put)
    }

    // 把put集合提交表对象
    table.put(putList)

  }

  def init(): Unit = {
    // 创建配置类
    val configuration: Configuration = HBaseConfiguration.create()
    // 设定地址
    configuration.set("hbase.zookeeper,quorum", HBASE_SERVER)
    // 创建连接
    connection = ConnectionFactory.createConnection(configuration)
  }

}
