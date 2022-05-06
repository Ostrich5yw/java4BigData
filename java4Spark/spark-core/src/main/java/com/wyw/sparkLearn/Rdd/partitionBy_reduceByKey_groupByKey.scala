package com.wyw.sparkLearn.Rdd

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/3 15:44
 */
object partitionBy_reduceByKey_groupByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4))
    val mapRDD = rdd.map((_, 1))
    val rdd2 = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 6), ("b", 1)))
    /**
     * partitionBy——根据指定的分区规则对数据进行重分区
     * 注意partitionBy不是RDD的封装方法。但是通过implict隐式类型转换，将RDD转化为PairRDDFunctions实现。
     * 1. 如果多次使用partitionBy进行分区，且使用的分区器和分区数一致，那么将不会进行再次分区
     * 2.
     * */
    val value = mapRDD.partitionBy(new HashPartitioner(partitions = 2)).saveAsTextFile("file/output")
    /**
     * reduceBy——相同的key进行value数据的聚合操作
     * 如果只有一个key，将不参加reduce而直接略过
     * */
    val value2 = rdd2.reduceByKey((x, y) => {x + y})
    value2.collect().foreach(println)
    /**
     * groupByKey——相同的key进行分组操作，形成对偶元组
     * 远足中第一个元素就是key，第二个元素是相同key的value集合
     * groupBy可以以任意参数进行分组，groupByKey只能以key进行分组
     * groupBy输出[K, Iterable[T]].groupByKey输出[K, Iterable[V]]
     * */
    val value3 = rdd2.groupByKey()
    value3.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
