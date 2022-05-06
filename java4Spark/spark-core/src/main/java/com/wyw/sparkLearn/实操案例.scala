package com.wyw.sparkLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/5 15:03
 */
object 实操案例 {
  /**
   * ➢ 数据文件中每行数据采用下划线分隔数据
   * ➢ 每一行数据表示用户的一次行为，这个行为只能是 4 种行为的一种
   * ➢ 如果搜索关键字为 null,表示数据不是搜索数据
   * ➢ 如果点击的品类 ID 和产品 ID 为-1，表示数据不是点击数据
   * ➢ 针对于下单行为，一次可以下单多个商品，所以品类 ID 和产品 ID 可以是多个，id 之
   *    间采用逗号分隔，如果本次不是下单行为，则数据采用 null 表示
   * ➢ 支付行为和下单行为类似
   * */
  case class UserVisitAction(
    date: String,//用户点击行为的日期
    user_id: Long,//用户的 ID
    session_id: String,//Session 的 ID
    page_id: Long,//某个页面的 ID
    action_time: String,//动作的时间点
    search_keyword: String,//用户搜索的关键词
    click_category_id: Long,//某一个商品品类的 ID
    click_product_id: Long,//某一个商品的 ID
    order_category_ids: String,//一次订单中所有品类的 ID 集合
    order_product_ids: String,//一次订单中所有商品的 ID 集合
    pay_category_ids: String,//一次支付中所有品类的 ID 集合
    pay_product_ids: String,//一次支付中所有商品的 ID 集合
    city_id: Long     //城市ID
  )
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.textFile("file/user_visit_action.txt")
    rdd.cache()

    //TODO 需求1：商品品类综合排名 = 点击数*20%+下单数*30%+支付数*50%，统计排名前十
    val clickRDD = rdd.filter(line => {       //分别计算针对三个指标，每个品类的数量
      val strings = line.split("_")
      strings(6) != "-1"
    }).map(line =>{
      val data = line.split("_")
      (data(6), 1)
    }).reduceByKey(_+_)
    val orderRDD = rdd.filter(line => {
      val strings = line.split("_")
      strings(8) != "null"
    }).flatMap(data => {
      val strings = data.split("_")
      strings(8).split(",").map(data2 => {
        (data2, 1)
      })
    }).reduceByKey(_+_)
    val payRDD = rdd.filter(line => {
      val strings = line.split("_")
      strings(10) != "null"
    }).flatMap(data => {
      val strings = data.split("_")
      strings(10).split(",").map(data2 => {
        (data2, 1)
      })
    }).reduceByKey(_+_)


    val value: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickRDD.cogroup(orderRDD, payRDD)    //组合
    value.mapValues{
      case (click, order, pay) => {
        var cCount = 0
        if(click.iterator.hasNext){
          cCount = click.iterator.next()
        }
        var oCount = 0
        if(order.iterator.hasNext){
          oCount = order.iterator.next()
        }
        var pCount = 0
        if(pay.iterator.hasNext){
          pCount = pay.iterator.next()
        }
        (cCount, oCount, pCount)
      }
    }.sortBy(_._2._1, false).take(10).foreach(println)

    //也可以一次完成三个的统计
    val res1 = rdd.flatMap(line => {
      val strings = line.split("_")
      if(strings(6) != "-1"){
        List((strings(6), (1, 0, 0)))
      }else if (strings(8) != "null"){
        var data = strings(8).split(",")
        data.map(da => (da, (0, 1, 0)))
      }else if (strings(10) != "null"){
        var data = strings(10).split(",")
        data.map(da => (da, (0, 1, 0)))
      }else
        Nil
    }).reduceByKey((x, y) => {(x._1 + y._1, x._2 + y._2, x._3 + y._3)}).sortBy(_._2._1, false).take(10)
    res1.foreach(println)

    //TODO 需求2：Top10 热门品类中每个品类的 Top10 活跃 Session 统计
    val rdd2: Array[String] = res1.map(_._1)
    rdd.filter(line => {
      val strings = line.split("_")
      if(strings(6)!= "-1")
        rdd2.contains(strings(6))
      else
        false
    }).map(line => {
      val strings = line.split("_")
      ((strings(6), strings(2)), 1)
    }).reduceByKey(_+_).map{
      case ((categorey, session), sum) => {
        (categorey, (session, sum))
      }
    }.groupByKey().mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    }).foreach(println)

    //TODO 需求3：计算页面单跳转化率，即用户浏览页面1的次数为A，紧接着浏览页面2的次数为B，则B/A为1-2的单跳转化率
    val actionRDD = rdd.map(line => {
      val data = line.split("_")
      UserVisitAction(
        data(0),data(1).toLong,data(2),data(3).toLong,data(4),data(5),data(6).toLong,data(7).toLong,data(8),data(9),data(10),data(11),data(12).toLong
      )
    })
    actionRDD.cache()
    /**
     * 计算分母
     * */
    val fenmu: Map[Long, Long] = actionRDD.map(data => {
      (data.page_id, 1L)
    }).reduceByKey(_ + _).collect().toMap
    /**
     * 计算分子
     * */
    val value1: RDD[List[((Long, Long), Int)]] = actionRDD.map(data => {
      (data.session_id, (data.action_time, data.user_id, data.page_id))
    }).groupByKey().mapValues(data => {
      var list = data.toList.sortBy(_._1)
      val longs = list.map(data => data._3)
      // [1,2,3,4]
      // [1,2],[2,3],[3,4]
      // 只需要[1,2,3,4] [2,3,4]计算zip即可
      val tuples = longs.zip(longs.tail)
      tuples.map(data => {
        (data, 1)
      })
    }).map(_._2)
    val fenzi: RDD[((Long, Long), Int)] = value1.flatMap(list => list).reduceByKey(_ + _)
    /**
     * 计算转化率
     * */
    fenzi.foreach{
      case ((bef, sec), num) => {
        val i = fenmu.getOrElse(bef, 0L)
        println(s"页面${bef}——页面${sec}的转化率: " + (num.toDouble / i))
      }
    }
  }
}
