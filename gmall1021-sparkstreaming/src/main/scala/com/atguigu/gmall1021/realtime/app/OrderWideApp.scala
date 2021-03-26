package com.atguigu.gmall1021.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1021.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall1021.realtime.util.{HbaseUtil, MyEsUtil, MyKafkaUtil, OffsetManageUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.time.{LocalDate, Period}
import java.util
import java.util.Date
import scala.collection.mutable.ListBuffer

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

    // 4 数据结构调整，把流中元素从record 调整为jsonObj
    // 主表 // 改成样例类
    val orderInfoDstream: DStream[OrderInfo] = orderInfoWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      // 补充日期和小时字段
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)
      orderInfo
    }
    // 从表
    val orderDetailDstream: DStream[OrderDetail] = orderDetailWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }
    // 打印
    //    orderInfoJsonObjDstream.print(100)
    //    orderDetailJsonObjDstream.print(100)

    val orderInfoWithUserDstream: DStream[OrderInfo] = orderInfoDstream.map { orderInfo =>
      // 需要补充用户字段
      val userId: String = orderInfo.user_id.toString
      val rowkey: String = HbaseUtil.getRowkey(userId)
      val userJsonObj: JSONObject = HbaseUtil.get("DIM_USER_INFO", rowkey)
      orderInfo.user_gender = userJsonObj.getString("gender")
      val birthday: LocalDate = LocalDate.parse(userJsonObj.getString("birthday"))
      val now: LocalDate = LocalDate.now()
      val period: Period = Period.between(birthday, now)
      val years: Int = period.getYears
      orderInfo.user_age = years
      orderInfo
    }

    // 完成province
    // 小表 考虑 直接把数据表从库中 加载到内存中，流中的数据逐个访问内存即可

    val orderInfoWithProvinceUserDstream: DStream[OrderInfo] = orderInfoWithUserDstream.transform { rdd =>
      // driver 每批次执行一次
      val provinceMap: Map[String, JSONObject] = HbaseUtil.getTableData("DIM_BASE_PROVINCE")
      // 封装广播变量
      val provinceMapBC: Broadcast[Map[String, JSONObject]] = ssc.sparkContext.broadcast(provinceMap)
      val orderInfoJSONRdd: RDD[OrderInfo] = rdd.map { orderInfo =>
        // 从订单流中获取订单的省市id
        val provinceId: String = orderInfo.province_id.toString
        // 把id转为rowkey
        val rowkey: String = HbaseUtil.getRowkey(provinceId)
        // 用rowkey从map中查询省市数据
        // executor
        // 从广播变量中提取数据
        val provinceMapFromBC: Map[String, JSONObject] = provinceMapBC.value
        val provinceObj: JSONObject = provinceMapFromBC.getOrElse(rowkey, null)
        // 提取省市数据的字段值
        if (provinceObj != null) {
          // 补充到订单中
          orderInfo.province_name = provinceObj.getString("name")
          orderInfo.province_area_code = provinceObj.getString("area_code")
          orderInfo.province_3166_2_code = provinceObj.getString("iso_3166_2")
          orderInfo.province_iso_code = provinceObj.getString("iso_code")
        }
        orderInfo
      }
      orderInfoJSONRdd
    }

    //    orderInfoWithProvinceUserDstream.print(100)
    //    orderDetailDstream.print(100)

    // join 之前要把结构调整为key-value的元组结构，其中key是关联的键
    val orderInfoWithIdDstream: DStream[(Long, OrderInfo)] = orderInfoWithProvinceUserDstream.map { orderInfo => (orderInfo.id, orderInfo) }
    val orderDetailWithOrderIdDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map { orderDetail => (orderDetail.order_id, orderDetail) }

    // 验证是否有数据
    //    orderInfoWithIdDstream.cache()
    //    orderDetailWithOrderIdDstream.cache()
    //    orderInfoWithIdDstream.print(1000)
    //    orderDetailWithOrderIdDstream.print(1000)


    val fullJoinDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithIdDstream.fullOuterJoin(orderDetailWithOrderIdDstream)

    val orderWideDstream: DStream[OrderWide] = fullJoinDstream.flatMap { case (orderId, (orderInfoOpt, orderDetailOpt)) =>
      val orderJoinedList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
      val jedis: Jedis = RedisUtil.getJedisClient
      val ORDER_INFO_PREFIX = "order_joined:order_info:"
      val ORDER_DETAIL_PREFIX = "order_joined:order_detail:"
      if (orderInfoOpt != None) {
        // 1 主表
        val orderInfo: OrderInfo = orderInfoOpt.get
        if (orderDetailOpt != None) {
          val orderDetail: OrderDetail = orderDetailOpt.get
          // 1.1 如果从表也有 把主表和从表合并
          orderJoinedList.append(new OrderWide(orderInfo, orderDetail))
        }

        // 1.2 主表写入缓存 采用string
        val key: String = ORDER_INFO_PREFIX + orderInfo.id
        val orderInfoJson: String = JSON.toJSONString(orderInfo, new SerializeConfig(true))
        jedis.setex(key, 600, orderInfoJson)

        // 1.3 主表查询从表的缓存，如果查到，合并
        val orderDetailKey: String = ORDER_DETAIL_PREFIX + orderInfo.id
        // 主表查询从表
        val orderDetailJsonSet: util.Set[String] = jedis.smembers(orderDetailKey)
        // 迭代从表的结果集 依次跟主表合并成宽表
        if (orderDetailJsonSet != null && orderDetailJsonSet.size() > 0) {
          import collection.JavaConverters._
          for (orderDetailJson <- orderDetailJsonSet.asScala) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            orderJoinedList.append(new OrderWide(orderInfo, orderDetail))

          }
        }


      } else {
        val orderDetail: OrderDetail = orderDetailOpt.get
        // 2 从表
        // 2.1 从表查询主表的缓存
        val orderInfoKey: String = ORDER_INFO_PREFIX + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        if (orderInfoJson != null && orderInfoJson.length > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          orderJoinedList.append(new OrderWide(orderInfo, orderDetail))
        } else {
          // 2.2 未查询到主表数据 从表写入缓存
          val orderDetailKey: String = ORDER_DETAIL_PREFIX + orderDetail.order_id
          val orderDetailJson: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))

          jedis.sadd(orderDetailKey, orderDetailJson)
          jedis.expire(orderDetailKey, 600)
        }

      }
      jedis.close()
      orderJoinedList
    }

    //    orderWideDstream.print(1000)

    // 将数据写入es
    orderWideDstream.foreachRDD { rdd =>
      rdd.foreachPartition { orderWideItr =>
        val orderWideList: List[OrderWide] = orderWideItr.toList
        if (orderWideList != null && orderWideList.size > 0) {
          // 去第一行的日期
          val create_date: String = orderWideList(0).create_date
          // 把本批次本分区的数据统一保存
          val orderWideWithIdList: List[(String, OrderWide)] = orderWideList.map(orderWide => (orderWide.detail_id.toString, orderWide))
          val indexName: String = "gmall1021_order_wide_" + create_date
          MyEsUtil.saveBulk(orderWideWithIdList, indexName)
        }
      }
      OffsetManageUtil.saveOffset(orderInfoTopic, groupId, orderInfoOffsetRanges)
      OffsetManageUtil.saveOffset(orderDetailTopic, groupId, orderDetailOffsetRanges)
    }

    //    OffsetManageUtil.saveOffset(orderInfoTopic,groupId,orderInfoOffsetRanges)
    //    OffsetManageUtil.saveOffset(orderDetailTopic,groupId,orderDetailOffsetRanges)

    ssc.start()
    ssc.awaitTermination()
  }

}
