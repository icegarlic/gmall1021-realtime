package com.atguigu.gmall1021.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall1021.realtime.util.{HbaseUtil, MyKafkaSender, MyKafkaUtil, OffsetManageUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDbCanalApp {


  //    1 从redis 读取偏移量
  //    2 按照偏移量从kafka中加载数据流
  //    3 从数据流中得到本批次的偏移量结束点，用于结束时提交偏移
  //    4 数据结构调整  把流中元素从record 调整为jsonObj
  //    5 按数据是否是维度进行拆分
  //      6.1  事实数据   按表+操作类型 进行拆分  存储到kafka 中
  //      6.2  维度数据   按表   进行拆分  存储到hbase中

  //    7 提交偏移量
  def main(args: Array[String]): Unit = {
    //    1 从redis 读取偏移量
    val topic = "ODS_BASE_DB_C"
    val groupId = "base_db_canal_group"
    val offsetMap: Map[TopicPartition, Long] = OffsetManageUtil.getOffset(topic, groupId)
    //    2 按照偏移量从kafka中加载数据流
    val sparkConf: SparkConf = new SparkConf().setAppName("base_db_canal_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) { //如果有偏移量值则按照偏移量位置取数据，如果没有偏移量，则按照默认取最新数据
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //    3 从数据流中得到本批次的偏移量结束点，用于结束时提交偏移
    var offsetRanges: Array[OffsetRange] = null
    val inputWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd => //周期性的在driver中执行的任务  每批次执行一次 //转换算子
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    //    4 数据结构调整  把流中元素从record 调整为jsonObj
    val logJsonDstream: DStream[JSONObject] = inputWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }

    val dimTableNames = Array("user_info", "base_province")
    val factTableNames = Array("order_detail", "order_info", "payment_info")

    //    5 按数据是否是维度进行拆分
    logJsonDstream.cache()

    //    logJsonDstream.print(100)
    logJsonDstream.foreachRDD { rdd =>
      rdd.foreach { jsonObj =>
        //判断数据属于维度还是事实
        val tableName: String = jsonObj.getString("table")
        val jSONArray: JSONArray = jsonObj.getJSONArray("data") //得到某个对象后转成JSONArray
        val dataJsonObj: JSONObject = jSONArray.getJSONObject(0) //得到某个下标的对象后转成JSONObject
        //获得主键的名称
        val pkName: String = jsonObj.getJSONArray("pkNames").getString(0)
        val pk: lang.Long = dataJsonObj.getLong(pkName)
        if (factTableNames.contains(tableName)) {
          //6.1 事实数据  分表分类型 写入kafka主题
          val optType: String = jsonObj.getString("type")
          val opt: String = optType.substring(0, 1)
          val topicName = "DWD_" + tableName.toUpperCase + "_" + opt // DWD_ORDER_INFO_I

          MyKafkaSender.send(topicName, dataJsonObj.toJSONString)
          //可以根据下游的业务操作 ，选择指定的分区键进行发送数据，减少下游shuffle操作的成本
          // MyKafkaSender.send(topicName,key,dataJsonObj.toJSONString)
          // Thread.sleep(200)
        } else if (dimTableNames.contains(tableName)) {
          //6.2  维度数据  分表写入hbase中
          //表名 tableName    DIM_USER_INFO
          //生成rowkey   00-19  20-39 40-59 60-79 80-99
          // 把顺序的序列进行反转
          //0000000105  110   2014                110001  110002  110003  110004  ..110009
          // 900011  800011 700011 600011 500011
          // 5010000000

          val rowkey: String = HbaseUtil.getRowkey(pk.toString)
          val hbaseTable = "DIM_" + tableName.toUpperCase
          // 列名和列值  dataJsonObj
          HbaseUtil.put(hbaseTable, rowkey, dataJsonObj)

        }
      }
      //    7 提交偏移量
      OffsetManageUtil.saveOffset(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination();
  }

}
