package com.wyw.sparkLearn.Rdd

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/3 17:37
 */
object aggregateByKey_foldByKey_combineByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 从集合（内存）中建立RDD
    val rdd = sc.makeRDD(List(1,2,3,4))
    val mapRDD = rdd.map((_, 1))
    val rdd2 = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 2),("a", 3), ("a", 4), ("b", 3)),2)
    /**
     * aggregateByKey——提供分区内和分区间不同规则的聚合操作
     * 存在函数柯里化，有两个参数列表
     * 第一个参数列表需要传递一个参数，表示为初始值（比如下例中，第一个输入的("a", 1)需要一个初始比较值，进行max比较）
     * 第二个参数列表需要传递两个参数
     *    1.分区内计算规则 (x, y)  x的格式与第一个参数一致，y的格式与rdd一致
     *    2.分区间计算规则  (x, y)  x,y格式均与第一个参数一致
     * */
    val value = rdd2.aggregateByKey(0)((x, y) => math.max(x, y), (x, y) => x + y) //先求每个分区内，相同key的Value最大值maxval，再求所有分区的maxval之和
    val value11 = rdd2.aggregateByKey((0, 0))(
      (t, v) => (t._1 + v, t._2 + 1),
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    )//统计每一个Key的value和，以及每一个Key的出现次数
    value11.collect().foreach(println)
    /**
     * foldByKey——分区内与分区间操作一致时，使用该函数
     * */
    val value2 = rdd2.foldByKey(0)((x, y) => x + y)
    value2.collect().foreach(println)
    /**
     * combinedByKey——类似于aggregate()，combineByKey()允许用户返回值的类型与输入不一致。
     * 需要三个参数
     * 第一个参数：将相同key的第一个数据进行结构的转换
     * 第二个参数：分区内计算规则
     * 第三个参数：分区间计算规则
     * */
    val value3 = rdd2.combineByKey(     //统计每一个Key的value和，以及每一个Key的出现次数
      v => (v, 1),
      (t:(Int, Int), v) => {(t._1 + v, t._2 + 1)},
      (t1:(Int, Int), t2:(Int, Int)) => {(t1._1 + t2._1, t1._2 + t2._2)}
    )
    value3.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
