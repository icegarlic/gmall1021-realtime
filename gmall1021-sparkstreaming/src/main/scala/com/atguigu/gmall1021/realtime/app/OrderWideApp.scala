package com.atguigu.gmall1021.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1021.realtime.util.{MyKafkaUtil, OffsetManageUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderWideApp {

  //    1 从redis 读取偏移量  俩个主题
  //    2 按照偏移量从kafka中加载数据流 俩个主题
  //    3 从数据流中得到本批次的偏移量结束点，用于结束时提交偏移 俩个主题
  //    4 数据结构调整  把流中元素从record 调整为jsonObj 俩个主题
  //    5 合并成宽表
  //    6 保存数据 写入es
  //    7 提交偏移量
  def main(args: Array[String]): Unit = {
    // 1 sparkstreaming 要能够消费到kafka
    val sparkConf: SparkConf = new SparkConf().setAppName("order_wide_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val orderInfoTopic = "DWD_ORDER_INFO_I"
    val orderDetailTopic = "DWD_ORDER_DETAIL_I"
    val groupId = "order_wide_group"

    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManageUtil.getOffset(orderInfoTopic, groupId)
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManageUtil.getOffset(orderDetailTopic, groupId)

    // 2 按照偏移量从kafka中加载数据流
    // 主表
    var orderInfoInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMap != null && orderInfoOffsetMap.size > 0) {
      orderInfoInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMap, groupId)
    } else {
      orderInfoInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, groupId)
    }
    // 从表
    var orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMap != null && orderDetailOffsetMap.size > 0) {
      orderDetailInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMap, groupId)
    } else {
      orderDetailInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, groupId)
    }

    // 3 从数据流中得到本批次的偏移量结束点，用于结束提交偏移量
    // 主表
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoWithOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoInputDstream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    // 从表
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailWithOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailInputDstream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // 4 护具结构调整，把流中元素从record 调整为jsonObj
    // 主表
    val orderInfoJsonObjDstream: DStream[AnyRef] = orderInfoWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }
    // 从表
    val orderDetailJsonObjDstream: DStream[AnyRef] = orderDetailWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }
    // 打印
    orderInfoJsonObjDstream.print(100)
    orderDetailJsonObjDstream.print(100)

//    OffsetManageUtil.saveOffset(orderInfoTopic,groupId,orderInfoOffsetRanges)
//    OffsetManageUtil.saveOffset(orderDetailTopic,groupId,orderDetailOffsetRanges)

    ssc.start()
    ssc.awaitTermination()
  }

}
