package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2022/3/4 15:44
 */
object 练习 {
  /**
   * agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
   * 需求描述
   * 统计出每一个省份每个广告被点击数量排行的 Top3
   *
   * */
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.textFile("file/agent.log")
    val mapRDD = rdd.map(line => {
      val data = line.split(" ")
      ((data(1), data(4)), 1)
    })
    val rdkRDD = mapRDD.reduceByKey(_+_)

    val mapRDD2 = rdkRDD.map {
      case ((t1, t2), t3) => {
        (t1, (t2, t3))
      }
    }

    val gbRDD = mapRDD2.groupByKey()

    val value = gbRDD.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })
    value.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
