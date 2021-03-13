package com.atguigu.gmall1021.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

object OffsetManageUtil {

  /**
   * 读取偏移量
   *
   * @param topic   主题
   * @param groupId 消费者组
   * @return 每个分区的偏移量
   */
  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = RedisUtil.getJedisClient
    // redis中 偏移量的存储结构
    // type？hash
    // key？ offset:[topic1]:[group1]
    // field？partitionNum
    // value？offset
    val key: String = "offset:" + topic + ":" + groupId
    val offsetMap: util.Map[String, String] = jedis.hgetAll(key)
    jedis.close()
    import collection.JavaConverters._
    val topPartitionMap: mutable.Map[TopicPartition, Long] = offsetMap.asScala.map { case (partition, offset) =>
      val topicPartition: TopicPartition = new TopicPartition(topic, partition.toInt)
      println("偏移量读取： 分区：" + partition + " 偏移量起始点：" + offset)
      (topicPartition, offset.toLong)
    }
    topPartitionMap.toMap
  }

  /**
   * 存储偏移量
   *
   * @param topic        主题
   * @param groupId      消费者
   * @param offsetRanges 每个分区的偏移量结束点
   */
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    val jedis: Jedis = RedisUtil.getJedisClient
    //redis中 偏移量的存储结构
    // type ?   hash
    // key? offset:[topic1]:[group1]
    // field ? partitionNum   value ? offset
    // 读取api hgetall    写入api ?      过期时间？不设过期
    // 1 把offsetRange转换成 redis的偏移量存储结构
    val offsetMap: util.Map[String, String] = new util.HashMap[String, String]
    for (offsetRange <- offsetRanges) {
      val partition: Int = offsetRange.partition
      val untilOffset: Long = offsetRange.untilOffset
      println("偏移量写入： 分区：" + partition + " 偏移量结束：" + untilOffset)
      offsetMap.put(partition.toString, untilOffset.toString)
    }
    //2  存储到redis中
    val key = "offset:" + topic + ":" + groupId
    jedis.hset(key, offsetMap)
    jedis.close()
  }

}
