package com.mouse.redisPool

import com.mouse.ExactlyOnce.GetLog.MyRecord
import com.mouse.ExactlyOnce.{GetLog, RedisClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Pipeline

/**
  * @author 咖啡不加糖
  */

/**
  * 注释：
  *
  * 在Spark Streaming中消费Kafka数据，保证Exactly-once的核心有三点：
  * 使用Direct方式连接Kafka；自己保存和维护Offset；更新Offset和计算在同一事务中完成；
  *
  *  思路步骤：
  *  1.启动后，先从Redis中获取上次保存的Offset，Redis中的key为”topic_partition”，即每个分区维护一个Offset；
  *  2.使用获取到的Offset，创建DirectStream；
  *  3.在处理每批次的消息时，利用Redis的事务机制，确保在Redis中指标的计算和Offset的更新维护，
  *  在同一事务中完成。只有这两者同步，才能真正保证消息的Exactly-once。
  *
  *  注意事项：
  *   1.在启动Spark Streaming程序时候，有个参数最好指定：
  *   2.spark.streaming.kafka.maxRatePerPartition=20000（每秒钟从topic的每个partition最多消费的消息条数）
  *   如果程序第一次运行，或者因为某种原因暂停了很久重新启动时候，会积累很多消息，
  *   如果这些消息同时被消费，很有可能会因为内存不够而挂掉，因此，需要根据实际的数据量大小，
  *   以及批次的间隔时间来设置该参数，以限定批次的消息量。
  *   3.如果该参数设置20000，而批次间隔时间未10秒，那么每个批次最多从Kafka中消费20万消息。
  *
  *
  */
object SparkStreaming_ExactlyOnce {
  def main(args: Array[String]): Unit = {
    val brokers = "kafka:9092"
    val topic = "save_redis_offset"
    val partition : Int = 0 //测试topic只有一个分区

    //Kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "exactly-once",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "auto.offset.reset" -> "none"
    )

    // Redis configurations
    val maxTotal = 10
    val maxIdle = 10
    val minIdle = 1
    val redisHost = "192.168.1.1"
    val redisPort = 6379
    val redisTimeout = 30000
    //默认db，用户存放Offset和pv数据
    val dbDefaultIndex = 8
    RedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)


    val conf = new SparkConf().setAppName("TestSparkStreaming").setIfMissing("spark.master", "local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))

    //从Redis获取上一次存的Offset
    val jedis = RedisClient.getPool.getResource
    jedis.select(dbDefaultIndex)
    val topic_partition_key = topic + "_" + partition
    var lastOffset = 0L
    val lastSavedOffset = jedis.get(topic_partition_key)

    if(null != lastSavedOffset) {
      try {
        lastOffset = lastSavedOffset.toLong
      } catch {
        case ex : Exception => println(ex.getMessage)
          println("get lastSavedOffset error, lastSavedOffset from redis [" + lastSavedOffset + "] ")
          System.exit(1)
      }
    }
    RedisClient.getPool.returnResource(jedis)

    println("lastOffset from redis -> " + lastOffset)

    //设置每个分区起始的Offset
    val fromOffsets = Map{new TopicPartition(topic, partition) -> lastOffset}

    //使用Direct API 创建Stream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
    )

    //开始处理批次消息
    stream.foreachRDD {
      rdd =>

        /**
          * 获取 RDD 每一个分区里的offset;
          * OffsetRange里面核心字段{
            val topic: String,    主题
            val partition: Int,   分区
            val fromOffset: Long, 该拉去数据的开始偏移量
            val untilOffset: Long 该分区拉去数据的最后偏移量
            }

          */
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val result: Array[MyRecord] = GetLog.processLogs(rdd)
        println("=============== Total " + result.length + " events in this batch ..")
        val jedis = RedisClient.getPool.getResource
        val p1 : Pipeline = jedis.pipelined();
        p1.select(dbDefaultIndex)
        p1.multi() //开启事务


        //逐条处理消息
        result.foreach {
          record =>
            //增加小时总pv
            val pv_by_hour_key = "pv_" + record.hour
            p1.incr(pv_by_hour_key)

            //增加网站小时pv
            val site_pv_by_hour_key = "site_pv_" + record.site_id + "_" + record.hour
            p1.incr(site_pv_by_hour_key)

            //使用set保存当天的uv
            val uv_by_day_key = "uv_" + record.hour.substring(0, 10)
            p1.sadd(uv_by_day_key, record.user_id)
        }

        //更新Offset
        offsetRanges.foreach { offsetRange =>
          println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
          val topic_partition_key = offsetRange.topic + "_" + offsetRange.partition
          p1.set(topic_partition_key, offsetRange.untilOffset.toString)
        }

        p1.exec();//提交事务
        p1.sync();//关闭pipeline

        RedisClient.getPool.returnResource(jedis)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
