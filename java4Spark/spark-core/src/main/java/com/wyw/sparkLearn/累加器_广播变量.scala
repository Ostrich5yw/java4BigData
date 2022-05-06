package com.wyw.sparkLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 *
 * @author 5yw
 * @date 2022/3/4 21:59
 */
object 累加器_广播变量 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4), 2)
    /**
     * 一般情况下我们计算分区和
     * */
    rdd.reduce(_+_)
    /**
     * 希望一种简单方式进行计算
     * 以下方法并不会改变sum，因为由于闭包的存在，sum可以被传到Executor，但是没有办法传回Driver
     **/
    var sum = 0
    rdd.foreach(num => {
      sum = sum + num
    })
    /**
     * 累加器的作用就是将sum在各Executor执行后传回Driver端，再进行整体sum,比如Executor1将sum+3，Executor2将sum+7，传回Driver后就是sum+3+7=10
     **/
    val sum1 = sc.longAccumulator(name = "Sum")   //sc.doubleAccumulator  sc.collectionAccumulator
    rdd.foreach(num => {
      sum1.add(num)
    })
    println(sum1.value)

    /**
     * 广播变量：为所有Executor发送一个只读变量
     * */
    val rdd2 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd3 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    val value = rdd2.join(rdd3)     //join会导致数据量几何增长
    value.collect().foreach(println)

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    rdd2.map{
      case (w, c) => {
        val l:Int = map.getOrElse(w, 0)   //通过map实现与join相同的功能，但是可能造成过多内存占用
        (w, (c,l))
      }
    }.collect().foreach(println)

    val bc = sc.broadcast(map)
    rdd2.map{
      case (w, c) => {
        val l:Int = bc.value.getOrElse(w, 0)    //利用广播变量共享内存，减少内存占用
        (w, (c,l))
      }
    }.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
