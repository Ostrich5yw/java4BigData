package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2022/3/2 15:13
 */
object mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4), numSlices = 2)
    val mpRdd: RDD[Int] = rdd.mapPartitionsWithIndex(    //对两个分区分别加载，并且每个分区内再进行map转换 （因此println只执行两次）
      (index, iter) => {
        if(index == 1){
          iter
        }else{
          Nil.iterator    //其他分区一律返回空
        }
      }
    )
    mpRdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
