package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2022/3/3 14:44
 */
object filter {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4))
    val value = rdd.filter(num => num % 2 != 0)
    value.collect().foreach(println)

    //TODO 从文件中建立RDD
    val rdd2 = sc.textFile("file/apache.log")
    val mapRDD = rdd2.map(line => {
      val data = line.split(" ")
      data(3)
    })
    val filRDD = mapRDD.filter(data => data.startsWith("17/05/2015"))
    filRDD.collect().foreach(println)
    // 或
    val value2 = rdd2.filter(data => {
      data.split(" ")(3).startsWith("17/05/2015")
    })
    value2.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
