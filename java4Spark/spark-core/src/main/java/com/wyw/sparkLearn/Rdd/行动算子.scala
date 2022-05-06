package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2022/3/4 16:56
 */
object 行动算子 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4), 2)

    /**
     * reduce
     * */
    val a = rdd.reduce(_+_)
    println(a)
    /**
     * collect 将不同分区的数据按照分区顺序采集到Driver端的内存中，形成数组
     * */
    val ints = rdd.collect()
    println(ints.mkString(","))
    /**
     * count 求rdd元素数量
     * */
    val l = rdd.count()
    println(l)
    /**
     * first 求rdd第一个元素
     * */
    val i = rdd.first()
    println(i)
    /**
     * take 求rdd元素前n个
     * */
    val ints1 = rdd.take(3)
    println(ints1.mkString(","))
    /**
     * takeOrdered 排序并求rdd元素前n个
     * */
    val ints2 = rdd.takeOrdered(3)(Ordering.Int.reverse)
    println(ints2.mkString(","))
    /**
     * aggregate 分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
     * */
    val i1 = rdd.aggregate(10)(_ + _, _ + _)
    println(i1)
    /**
     * fold
     * */
    val i2 = rdd.fold(10)(_ + _)
    println(i2)
    /**
     * countByKey
     * */
    val rdd2 = sc.makeRDD(List(("a", 1), ("a", 2), ("n", 3)))
    val tupleToLong = rdd2.countByValue()
    val stringToLong = rdd2.countByKey()
    println(tupleToLong)
    println(stringToLong)
    /**
     * saveAsTextFile()、saveAsObjectFile(）、saveAsSequenceFile(）——针对键值对
     * */
    rdd.saveAsTextFile("file/output")
    rdd.saveAsObjectFile("file/output2")
    rdd2.saveAsSequenceFile("file/output3")
    /**
     * foreach
     * */
    rdd.collect().foreach(println)      //Driver端内存中进行循环遍历
    println("***************")
    rdd.foreach(println)                //Executor端内存进行循环遍历


    //TODO 关闭环境
    sc.stop()
  }
}
