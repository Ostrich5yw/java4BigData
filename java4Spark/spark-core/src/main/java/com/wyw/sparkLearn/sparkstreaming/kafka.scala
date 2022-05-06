package com.wyw.sparkLearn.sparkstreaming

import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.Predef._

/**
 *
 * @author 5yw
 * @date 2022/3/9 11:21
 */
object kafka {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamning")
    val sc = new StreamingContext(sparkconf, Seconds(3))

    //TODO 逻辑运算
    //1.定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ArrowAssoc(BOOTSTRAP_SERVERS_CONFIG) ->
        "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      GROUP_ID_CONFIG -> "5yw",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //2.读取 Kafka 数据创建 DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](sc,
                                                        LocationStrategies.PreferConsistent,
                                                          ConsumerStrategies.Subscribe[String, String](Set("5yw"), kafkaPara))
    //3.将每条消息的 KV 取出
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
    valueDStream.print()

    //TODO 开始执行
    sc.start()
    sc.awaitTermination()
  }
}
