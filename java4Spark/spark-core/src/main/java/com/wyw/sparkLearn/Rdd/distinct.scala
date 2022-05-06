package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/3 15:04
 */
object distinct {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,2,3,3,4))
    val value = rdd.distinct()
    value.collect().foreach(println)
    /**
     * 将List变换为如下的元组（tuple）（1, null）（2, null）（2, null）（3, null）（3, null）（4, null）
     *  进行reduceBykey操作，即根据key值进行聚合
     * （1, null）（2, null）（3, null）（4, null）使用map(_._1)变为 1,2,3,4
     * */
    //TODO 关闭环境
    sc.stop()
  }
}
