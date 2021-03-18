package com.atguigu.gmall1021.realtime.app

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall1021.realtime.util.{MyKafkaSender, MyKafkaUtil, OffsetManageUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDbCanalApp {
  //  1 从redis 读取偏移量
  //  2 按照偏移量kafka中加载数据流
  //  3 从数据流中得到本批次的偏移量结束点，用于结束时提交偏移量
  //  4 数据结构调整  把流中元素从record 调整为jsonObj
  //  5 按数据是否事维度进行拆分
  //  6.1 事实数据  按表+加操作类型 进行拆分 存储到kafka中
  //  6.2 维度表    按表          进行拆分 存储到hbase中

  //  8 提交偏移量


  def main(args: Array[String]): Unit = {
    // 1 从redis 读取偏移量
    val topic = "ODS_BASE_DB_C"
    val groupId = "base_db_canal_group"
    val offsetMap: Map[TopicPartition, Long] = OffsetManageUtil.getOffset(topic, groupId)

    // 2 按照偏移量从kafka中加载数据流
    val sparkConf: SparkConf = new SparkConf().setAppName("base_db_canal_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 3 从数据流中得到本批次的偏移量结束点，用于结束时提交偏移量
    var offsetRanges: Array[OffsetRange] = null // 存放位置 driver
    val inputWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd => // 周期性在driver中执行
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // driver
      rdd
    }

    // 4 数据结构调整 把流中元素从record 调整为jsonObj
    val logJsonDstream: DStream[JSONObject] = inputWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }

    val dimTableNames = Array("user_info", "sku_info", "base_province")
    val factTableNames = Array("order_detail", "order_info", "payment_info")

    // 5 根据数据情况进行分离
    logJsonDstream.foreachRDD { rdd =>
      rdd.foreach { jsonObj =>
        // 判断数据属于维度还是事实
        val tableName: String = jsonObj.getString("table")
        if (factTableNames.contains(tableName)) {
          // 事实数据 分表分类型 写入kafka主题
          val optType: String = jsonObj.getString("type")
          val opt: String = optType.substring(0, 1)
          val topicName: String = "DWD_" + tableName.toUpperCase + "_" + opt  //DWD_ORDER_INFO_I
          val jSONArray: JSONArray = jsonObj.getJSONArray("data") // 得到某个对象后转成JSONArray
          val dataJsonObj: JSONObject = jSONArray.getJSONObject(0)

          MyKafkaSender.send(topicName,dataJsonObj.toJSONString)
        }
      }
      // 维度数据 分表写入hbase中
    // 8 提交偏移量
      OffsetManageUtil.saveOffset(topic, groupId, offsetRanges)
    }




    ssc.start()
    ssc.awaitTermination()
  }
}
