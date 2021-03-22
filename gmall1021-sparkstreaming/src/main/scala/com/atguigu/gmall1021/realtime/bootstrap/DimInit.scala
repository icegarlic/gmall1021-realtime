package com.atguigu.gmall1021.realtime.bootstrap

import java.util

import com.alibaba.fastjson.JSONObject
import com.atguigu.gmall1021.realtime.util.{MyKafkaSender, MysqlUtil}

object DimInit {

  def main(args: Array[String]): Unit = {
    //1  通过jdbc读取mysql 对应的维度表
    val dimTables = Array("base_province", "user_info", "sku_info")
    for (dimTable <- dimTables) {
      val sql = "select * from " + dimTable
      val dimDataList: util.List[JSONObject] = MysqlUtil.queryList(sql)

      //2  逐行写入kafka      格式必须和canal的格式相同
      import collection.JavaConverters._
      for (dataJsonObj <- dimDataList.asScala) {
        val data = Array(dataJsonObj)
        val pkNames = Array("id")
        val table = dimTable
        val `type` = "INSERT"

        val messageJson = new JSONObject()
        messageJson.put("data", data)
        messageJson.put("pkNames", pkNames)
        messageJson.put("table", table)
        messageJson.put("type", `type`)

        //写入kafka
        MyKafkaSender.send("ODS_BASE_DB_C", messageJson.toJSONString)
      }
    }
    //一定要把producer  close吗
    // 如果不主动关闭 ，当main执行结束时， producer缓冲区的还未来得及发送的数据会丢失掉。
    //所以 在程序关闭前，要主动的把缓冲区的数据最后清空发送，保证不会丢失数据。
    MyKafkaSender.close
  }

}
