package com.wyw.sparkLearn.Rdd

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2022/3/3 10:10
 */
object groupby {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4), 2)
    /**
     * groupby 会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组，相同key值的数据会放置在一个组中
     * */
    def groupfunc(num: Int): Int = {
      num % 2
    }
    val groupRDD:RDD[(Int, Iterable[Int])] = rdd.groupBy(groupfunc)
    groupRDD.collect().foreach(println)

    //TODO 从集合（内存）中建立RDD
    val rdd1 = sc.makeRDD(List("Hello", "Spark", "Hadoop", "Scala"), 2)
    val groupRDD1:RDD[(Char, Iterable[String])] = rdd1.groupBy(_.charAt(0))
    groupRDD1.collect().foreach(println)

    //TODO 从文件中建立RDD，统计每天固定时间段的访问量
    val rdd2 = sc.textFile("file/apache.log")

    val gbRDD = rdd2.map(line => {
      val data = line.split(" ")
      val time = data(3)
      val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date: Date = sdf.parse(time)
      val sdf1 = new SimpleDateFormat("HH")
      val hour: String = sdf1.format(date)
      (hour, 1)
    }).groupBy(_._1) //以map中第一个参数进行分组
    val value = gbRDD.map(data => {
      (data._1, data._2.size)
    })
    val value1 = value.sortBy(data => {
      data._1
    })
    value1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
