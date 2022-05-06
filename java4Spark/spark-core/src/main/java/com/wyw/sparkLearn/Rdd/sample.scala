package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2022/3/3 14:53
 */
object sample {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
    val value = rdd.sample(
      //抽取后是否将数据返回，即是否会重复抽取
      withReplacement = false,
      //数据源中每条数据被抽取的概率
      fraction = 0.4,
      //抽取时随机算法的种子, 如果规定后，每一次抽取的结果将会相同；反之会使用系统时间作为种子，所以结果会不同
      seed = 1
    )
    value.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
