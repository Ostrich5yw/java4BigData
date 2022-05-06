package com.wyw.sparkLearn

import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.ConsumerConfig.{BOOTSTRAP_SERVERS_CONFIG, GROUP_ID_CONFIG}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ListBuffer

/**
 *
 * @author 5yw
 * @date 2022/3/9 19:59
 */
object sparkstreaming实操案例_业务逻辑 {
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
    val kafkadata: DStream[String] = kafkaDStream.map(record => record.value())
    val clickData = kafkadata.map(line => {
      val strings = line.split(" ")
      clickAction(strings(0), strings(1), strings(2), strings(3), strings(4))
    })

    //TODO 业务一：记录每天对某个广告点击超过30次的用户
    /**
     *  周期性获取黑名单数据，判断用户是否在黑名单中，如果不在，开始统计该用户本轮点击次数
     *  如果总次数超过阈值，将该用户拉入黑名单
     *  如果没有，对该用户当天的广告点击次数进行更新
     * */
    val nowData: DStream[((String, String, String), Int)] = clickData.transform(rdd => {
      var list_black = ListBuffer[String]()
      //通过JDBC周期性获取黑名单数据
      val conn = JDBCUtil.getConnection
      val ps = conn.prepareStatement("select userid from black_list")
      val res = ps.executeQuery()
      while (res.next()) {
        list_black.append(res.getString(1))
      }
      res.close()
      ps.close()
      conn.close()
      //判断rdd中的用户是否在黑名单中,已经在黑名单中则不必继续判断
      val filterRDD = rdd.filter(data => {
        !list_black.contains(data.userid)
      })
      //不在黑名单中的用户，记录其为(day, user, ad)，1的格式，并进行聚合，得到本轮数据中的统计值
      filterRDD.map(data => {
        val day = new SimpleDateFormat("yyyy-MM-dd").
          format(new java.util.Date(data.timestamp.toLong))
        val user = data.userid
        val ad = data.clickid
        ((day, user, ad), 1)
      })
    }).reduceByKey(_ + _)

    nowData.foreachRDD(rdd => {
          rdd.foreach {
            case ((day, user, ad), count) => {
              if (count >= 30) {    //超过30次则加入黑名单
                val conn = JDBCUtil.getConnection
                val ps = conn.prepareStatement(
                  """
                  | insert into black_list(userid) values (?)
                  | ON DUPLICATE_KEY
                  | UPDATE userid = ?
                  """.stripMargin)
                ps.setString(1, user)
                ps.setString(2, user)
                ps.executeUpdate()
                ps.close()
                conn.close()
              } else {  //没有超过，则需要查询表数据中的今日历史点击次数，如果和超过则拉入黑名单，否则更新表数据
                val conn = JDBCUtil.getConnection
                val ps = conn.prepareStatement(
                  """
                    | select * from user_ad_count where dt = ? and userid = ? and adid = ?
                  """.stripMargin)
                ps.setString(1, day)
                ps.setString(2, user)
                ps.setString(3, ad)
                val res = ps.executeQuery()
                if(res.next()){   //如果存在，判断和是否大于30
                  val ps1 = conn.prepareStatement(
                    """
                      | update user_ad_count set count = count + ? where dt = ? and uerid = ? and adid = ?
                    """.stripMargin)
                  ps1.setInt(1, count)
                  ps1.setString(2, day)
                  ps1.setString(3, user)
                  ps1.setString(4, ad)
                  ps1.executeUpdate()
                  ps1.close()
                  val ps12 = conn.prepareStatement(
                       """
                      | select * from user_ad_count where dt = ? and uerid = ? and adid = ? and count >= 30
                      |
                       """.stripMargin
                  )
                  ps12.setString(1, day)
                  ps12.setString(2, user)
                  ps12.setString(3, ad)
                  val res2 = ps12.executeQuery()
                  if(res2.next()){
                    val ps13 = conn.prepareStatement(
                      """
                        | insert into black_list(userid) values (?)
                        | ON DUPLICATE_KEY
                        | UPDATE userid = ?
                      """.stripMargin)
                    ps13.setString(1, user)
                    ps13.setString(2, user)
                    ps13.executeUpdate()
                    ps13.close()
                  }
                  res2.close()
                  ps12.close()
                }else {//不存在，说明一定不超过30，直接将新纪录插入表中
                  val ps2 = conn.prepareStatement(
                    """
                    | insert int user_ad_count (dt, userid, adid, count) values (?,?,?,?)
                    """.stripMargin)
                  ps2.setString(1, day)
                  ps2.setString(2, user)
                  ps2.setString(3, ad)
                  ps2.setInt(4, count)
                  ps2.executeUpdate()
                  ps2.close()
                }
                res.close()
                ps.close()
                conn.close()
              }
            }
          }
    })
    //TODO 开始执行
    sc.start()
    sc.awaitTermination()
  }
  case class clickAction(
    timestamp:String,
    area:String,
    city:String,
    userid:String,
    clickid: String
  )
}
