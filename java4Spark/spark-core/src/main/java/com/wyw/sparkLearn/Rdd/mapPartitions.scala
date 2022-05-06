package com.wyw.sparkLearn.Rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/2 11:31
 */
object mapPartitions {   //以分区为单位将对象加载到内存（引用数据），但是不会进行内存释放，所以数据量较大时不推荐。map来一条数据处理一条，不存在引用
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4), numSlices = 2)
    val mpRdd: RDD[Int] = rdd.mapPartitions(    //对两个分区分别加载，并且每个分区内再进行map转换 （因此println只执行两次）
      iter => {
        println(">>>>>>>>>>>")
        iter.map(_ * 2)     //每个数*2

        List(iter.max).iterator   //取每个分区
      }
    )
    mpRdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }

}
