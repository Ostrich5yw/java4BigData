package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2022/3/3 21:23
 */
object join_leftOuterJoin_cogrpup {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    /**
     * join
     * 将两个rdd中相同key的元素连接在一起，如果有多个匹配则类似于笛卡尔积
     * */
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    val rdd3 = sc.makeRDD(List(("a", 4), ("a", 5), ("b", 5)))
    val value1 = rdd.join(rdd2)
    value1.collect().foreach(println)
    /**
     * leftOuterJoin
     * rightOuterJoin
     * 左外连接与右外连接
     * */
    val value2 = rdd.leftOuterJoin(rdd3)
    val value3 = rdd.rightOuterJoin(rdd3)
    value2.collect().foreach(println)
    value3.collect().foreach(println)
    /**
     * cogroup
     * 先分组后，再进行左外连接
     * */
    val value = rdd.cogroup(rdd3)
    value.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
