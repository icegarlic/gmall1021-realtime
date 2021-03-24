package com.atguigu.gmall1021.realtime.util

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object HbaseUtil {

  private val properties: Properties = PropertiesUtil.load("config.properties")

  val DEFAULT_NAMESPACE = "GMALL1021"
  val DEFAULT_FAMILY = "INFO"
  val HBASE_SERVER = properties.getProperty("hbase.server")

  var connection: Connection = null

  /*
  写入方法
   */
  def put(tableName: String, rowKey: String, dataJsonObj: JSONObject): Unit = {
    if (connection == null) init() // 判断有没有连接 如果米有创建
    //从连接中获得表对象
    val table: Table = connection.getTable(TableName.valueOf(DEFAULT_NAMESPACE + ":" + tableName))
    val putList: util.List[Put] = new util.ArrayList[Put]()
    //组合多个put, 每个put代表提交一个字段
    //用什么进行循环
    import collection.JavaConverters._
    for (entry <- dataJsonObj.entrySet().asScala) {
      val columnName: String = entry.getKey
      var columnValue: String = ""
      if (entry.getValue != null) {
        columnValue = entry.getValue.toString
        //创建put动作

      }
      val put: Put = new Put(Bytes.toBytes(rowKey)).
        addColumn(Bytes.toBytes(DEFAULT_FAMILY),
          Bytes.toBytes(columnName),
          Bytes.toBytes(columnValue))
      putList.add(put)

    }

    //把put集合提交给表对象
    table.put(putList)

  }

  /**
   * 查询方法
   * @param tableName
   * @param rowkey
   * @return
   */
  def get(tableName: String, rowkey: String): JSONObject = {
    if (connection == null) init() // 判断有没有连接 如果米有创建
    val table: Table = connection.getTable(TableName.valueOf(DEFAULT_NAMESPACE + ":" + tableName))

    val get = new Get(Bytes.toBytes(rowkey))
    val result: Result = table.get(get)
    convertToJsonObj(result)
  }

  /**
   * 整表查询
   *
   * @param tableName
   * @return map  key=rowkey  ，value 字段和值
   */
  def getTableData(tableName: String): Map[String, JSONObject] = {
    if (connection == null) init() // 判断有没有连接 如果米有创建
    val table: Table = connection.getTable(TableName.valueOf(DEFAULT_NAMESPACE + ":" + tableName))

    val scan = new Scan
    val resultScanner: ResultScanner = table.getScanner(scan)
    val resultList: ListBuffer[(String, JSONObject)] = ListBuffer[(String, JSONObject)]()
    import collection.JavaConverters._
    //相当于把每行的数据进行提取转换
    for (result <- resultScanner.iterator().asScala) {
      val jsonObj: JSONObject = convertToJsonObj(result)
      val rowkey: String = Bytes.toString(result.getRow)
      resultList.append((rowkey, jsonObj))
    }
    resultList.toMap
  }

  //把result转换成jsonObj
  def convertToJsonObj(result: Result): JSONObject = {
    val cells: Array[Cell] = result.rawCells()
    //每个cell就是一个col-value
    val jsonObj = new JSONObject()
    for (cell <- cells) {
      //通过工具类从cell中提取 列名和列值
      val colName: String = Bytes.toString(CellUtil.cloneQualifier(cell))
      val colValue: String = Bytes.toString(CellUtil.cloneValue(cell))
      jsonObj.put(colName, colValue)
    }
    jsonObj
  }


  def init(): Unit = {
    //创建配置类
    val configuration: Configuration = HBaseConfiguration.create()
    //设定地址
    configuration.set("hbase.zookeeper.quorum", HBASE_SERVER)
    //创建连接
    connection = ConnectionFactory.createConnection(configuration)

  }

  //统一规则生成 rowkey
  def getRowkey(id: String): String = {
    var rowkey = StringUtils.leftPad(id.toString, 10, "0").reverse
    rowkey
  }

}
