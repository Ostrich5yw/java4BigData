package com.wyw.sparkLearn.Rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/1 22:06
 */
object 创建RDD {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val seq = Seq[Int](elems = 1,2,3,4)
    val rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(println)
    //TODO 从文件中建立RDD
    var fileRDD: RDD[String] = sc.textFile("./java4Spark/spark-core/src/main/resources/file/input.txt") //可以是具体文件名或目录名
//    sc.wholeTextFiles("") //可以识别文件是从hdfs还是硬盘上读的
    fileRDD.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
