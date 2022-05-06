package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 *
 * @author 5yw
 * @date 2022/3/4 21:25
 */
object cache_persist_checkpoint {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4), 2)
    val res1 = rdd.map(num => num + 1)

    res1.cache()      //缓存RDD数据
    res1.persist(StorageLevel.MEMORY_ONLY)    //更改缓存级别

    res1.checkpoint()   //设置检查点(数据落盘)

    val res2 = res1.map(num => num + 1)
    val res3 = res1.map(num => num + 2)

    res1.collect().foreach(println)
    res2.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
