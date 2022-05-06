package com.wyw.sparkLearn.Rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/2 11:07
 */
object map {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4))
    def mapFunction(num:Int): Int = {
      num * 2
    }
    val rddMap: RDD[Int] = rdd.map(mapFunction)
    // 可以简化为 rdd.map(num => num*2)
    rddMap.collect().foreach(println)

    //TODO 从文件中建立RDD
    val rdd2 = sc.textFile("file/apache.log")
    val rddMap2 = rdd2.map(
      line => {
        var datas = line.split(" ")
        datas(6)
      }
    )
    rddMap2.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
