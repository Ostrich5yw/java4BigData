package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2022/3/3 15:32
 */
object intersection_union_subtract_zip {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4,6,7,12))
    val rdd2 = sc.makeRDD(List(1,2,3,4,5,10,11))
    val value1 = rdd.intersection(rdd2)   //交集
    val value2 = rdd.union(rdd2)          //并集(包含二者的所有元素，不去重)
    val value3 = rdd.subtract(rdd2)       //寻找第一个rdd中独有的元素
    val value4 = rdd.zip(rdd2)            //加载成键值对 rdd数据必须一样多
    value1.collect().foreach(println)
    value2.collect().foreach(println)
    value3.collect().foreach(println)
    value4.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }
}
