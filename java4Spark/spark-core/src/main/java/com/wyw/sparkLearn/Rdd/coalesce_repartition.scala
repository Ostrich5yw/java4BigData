package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/3 15:09
 */
object coalesce_repartition {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4,5,6), 3)
    val value = rdd.coalesce(2)
    value.saveAsTextFile("file/output")
    value.collect().foreach(println)
    /**
     * 执行后发现，1，2在一个分区，3,4,5,6在另一个分区
     * 因为coalesce是按照分区将其划分为目标分区，即原先在一个分区的元素还会分配在同一分区
     * 这可能造成数据倾斜
     * */
    val value2 = rdd.coalesce(2, shuffle = true)  //打乱分区数据后进行分区，可以保障每一个新分区的元素数量一致
    /**
     * coalesce 缩小分区
     * repartition 扩大分区
     * */
    val value3 = rdd.repartition(7)
    //TODO 关闭环境
    sc.stop()
  }
}
