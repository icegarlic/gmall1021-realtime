package com.atguigu.gmall1021.realtime.app

import com.atguigu.gmall1021.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {

  def main(args: Array[String]): Unit = {
    // 1 sparkstreaming 要能够消费到kafka
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 2 通过 工具类 获得kafka数据流
    val topic = "ODS_BASE_LOG"
    val groupId = "dau_group"
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    inputDstream.map(_.value()).print()

    // 3 统计用户当日的首次访问 dau uv
    //    1 可以通过判断 日志中page 栏位是否last_page_id来决定该页面 --> 一次访问会话的首个页面
    //    2 也可以通过启动日志来判断  是否首次访问

    // 要把访问会话次数 --> 当日的首次访问（会话）

    ssc.start()
    ssc.awaitTermination()
  }
}
