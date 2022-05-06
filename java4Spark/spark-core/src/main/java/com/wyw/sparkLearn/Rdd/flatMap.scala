package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2022/3/2 15:21
 */
object flatMap {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(List(1,2), 3, List(3,4)))
    val fmRdd = rdd.flatMap(
      data => {
        data match {
          case list:List[_] => list
          case dat => List(dat)
        }
      }
    )
    fmRdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
