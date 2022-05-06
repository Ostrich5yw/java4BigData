package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/3 15:22
 */
object sortBy {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(("1", 1), ("2", 2), ("11", 3)))
    val value = rdd.sortBy(num => num._1, ascending = true)   //第二个参数绝定降序还是升序，默认为升序（false）
    value.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
