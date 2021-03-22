package com.atguigu.gmall1021.realtime.util

import com.alibaba.fastjson.JSONObject

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.util.Properties

object MysqlUtil {

  def main(args: Array[String]): Unit = {
    val list: java.util.List[JSONObject] = queryList("select * from order_info")
    println(list)
  }

  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val MYSQL_URL: String = properties.getProperty("mysql.url")
  private val MYSQL_USERNAME: String = properties.getProperty("mysql.username")
  private val MYSQL_PASSWORD: String = properties.getProperty("mysql.password")

  def queryList(sql: String): java.util.List[JSONObject] = {
    Class.forName("com.mysql.jdbc.Driver")
    // 创建结果列表
    val resultList: java.util.List[JSONObject] = new java.util.ArrayList[JSONObject]()
    // 创建连接
    val conn: Connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD)
    // 创建会话
    val stat: Statement = conn.createStatement
    println(sql)
    // 提交sql 返回结果
    val rs: ResultSet = stat.executeQuery(sql)
    // 为了获得列名 要取元素据
    val md: ResultSetMetaData = rs.getMetaData
    while (rs.next) {
      val rowData = new JSONObject();
      for (i <- 1 to md.getColumnCount) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList.add(rowData)
    }

    stat.close()
    conn.close()
    resultList
  }

}
