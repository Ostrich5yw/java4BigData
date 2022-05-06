package com.wyw.sparkLearn

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig.{BOOTSTRAP_SERVERS_CONFIG, GROUP_ID_CONFIG}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 *
 * @author 5yw
 * @date 2022/3/9 17:24
 */
object sparkstreaming实操案例_数据准备 {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val kafkaProducer = new KafkaProducer[String, String](prop)
    while(true){
      mockData().foreach(
        data => {
          val record = new ProducerRecord[String, String]("5yw", data)
          kafkaProducer.send(record)
        }
      )}
  }

  def mockData() = {
    // 生成数据类型
    // timestamp area city userid adid    表示为固定时间某个城市区域的某个用户点击了某个广告
    // 时间戳 区域 城市 用户 广告
    val list = ListBuffer[String]()
    val areaList = ListBuffer[String]("华北", "华东", "华南")
    val cityList = ListBuffer[String]("北京", "上海", "深圳")
    for(i <- 1 to 30){
      val area = areaList(new Random().nextInt(3))
      val city = areaList(new Random().nextInt(3))
      val userid = new Random().nextInt(6)
      val adid = new Random().nextInt(6)
      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }
    list
  }
}
