package com.atguigu.gmall1021.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1021.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object DauApp {

  def main(args: Array[String]): Unit = {
    // 1 sparkstreaming 要能够消费到kafka
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    /////////////////////
    /// a 此处完成读取redis中的偏移量
    ////////////////////

    // 2 通过 工具类 获得kafka数据流
    val topic = "ODS_BASE_LOG"
    val groupId = "dau_group"
    /////////////////////
    /// b 此处改造通过偏移量从指定位置消费kafka数据
    ////////////////////
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    //    inputDstream.map(_.value()).print(100)


    ////////////////////
    /// c 此处在数据量转换前，得到偏移量的结束点
    ///////////////////

    // 3 统计用户当日的首次访问 dau uv
    //    1 可以通过判断 日志中page 栏位是否last_page_id来决定该页面 --> 一次访问会话的首个页面
    //    2 也可以通过启动日志来判断  是否首次访问
    // 先转换格式，转换方便操纵的jsonObject
    val logJspnDstream: DStream[JSONObject] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val logJsonObj: JSONObject = JSON.parseObject(jsonString)
      val ts: lang.Long = logJsonObj.getLong("ts")
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateHourString: String = simpleDateFormat.format(new Date(ts))
      val dt: String = dateHourString.split(" ")(0)
      val hr: String = dateHourString.split(" ")(1)
      logJsonObj.put("dt", dt)
      logJsonObj.put("hr", hr)
      logJsonObj
    }

    // 过滤 得到每次会话的第一个访问页面
    val firstPageJsonDstream: DStream[JSONObject] = logJspnDstream.filter { logJsonObj =>

      var isFlagPage = false
      val pageJsonObj: JSONObject = logJsonObj.getJSONObject("page")
      if (pageJsonObj != null) {
        val lastPageId: AnyRef = pageJsonObj.get("last_page_id")
        if (lastPageId == null) {
          isFlagPage = true
        }
      }
      isFlagPage
    }

    //    firstPageJsonDstream.print(1000)

    // 要把访问会话次数 --> 当日的首次访问（日活）
    // 要如何去重：本质来说就是一种识别，识别每条日志对于当日来说是不是已经来过
    // 如何保存用户访问清单 ： redis
//    val dauJsonDstream: DStream[JSONObject] = firstPageJsonDstream.filter { logJsonObj =>
//      val jedis = RedisUtil.getJedisClient // 转换成连接池连接，减少开辟连接的次数
//      val dauKey = "dau:" + logJsonObj.getString("dt") // 设定key，每天一个清单，每个日期一个key
//      val mid: String = logJsonObj.getJSONObject("common").getString("mid")
//      val isFirstVisit: lang.Long = jedis.sadd(dauKey, mid)
//      jedis.close()
//      if (isFirstVisit == 1L) {
//        println("用户：" + mid + "首次访问")
//        true
//      } else {
//        println("用户：" + mid + "已经重复，去掉")
//        false
//      }
//    }

    val dauDstream: DStream[JSONObject] = firstPageJsonDstream.mapPartitions { jsonItr =>

      val jedis = RedisUtil.getJedisClient // 转换成连接池连接，减少开辟连接的次数
      val dauList = new ListBuffer[JSONObject]
      for (logJsonObj <- jsonItr) {
        val dauKey = "dau:" + logJsonObj.getString("dt") // 设定key，每天一个清单，每个日期一个key
        val mid: String = logJsonObj.getJSONObject("common").getString("mid")
        val isFirstVisit: lang.Long = jedis.sadd(dauKey, mid)
        if (isFirstVisit == 1L) {
          println("用户：" + mid + "首次访问")
          dauList.append(logJsonObj)
        } else {
          println("用户：" + mid + "已经重复，去掉")
          false
        }
      }
      jedis.close()
      dauList.toIterator

    }

    dauDstream.print(1000)
    ////////////////////////
    /// d 此处把偏移量的结束点，更新到redis中，作为偏移量的提交
    ////////////////////////

    ssc.start()
    ssc.awaitTermination()
  }
}
