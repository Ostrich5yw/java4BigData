package com.wyw.sparkLearn.Rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/3 8:28
 */
object glom {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4), 2)
    val dataRDD1:RDD[Array[Int]] = rdd.glom()
    val value = dataRDD1.map(data => {
      data.max
    })
    println(value.collect().sum)

    //TODO 关闭环境
    sc.stop()
  }
}
