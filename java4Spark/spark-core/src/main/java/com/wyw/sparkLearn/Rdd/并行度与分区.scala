package com.wyw.sparkLearn.Rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2022/3/2 10:10
 */
object 并行度与分区 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建rdd分区
    //第二个参数表示分区数量(可以并行计算的任务数量)
    //rdd分区内是串行的，分区间是并行的
    val rdd = sc.makeRDD(List(1,2,3,4), numSlices = 3)
    /**
     *** Spark分区时，规定最小分区数。然而由于hadoop特性，所以有时会超过。
     * 如（末尾为回车换行）
     * 1@@
     * 2@@
     * 3
     * 文件共有7个字节，所以7/2(目标分区) = 3（目标分区大小）
     * 7/3=2.1...   根据hadoop原则，超过部分多于10%需要新建新分区，所以最终分区数为3
     *
     *** Spark 读取文件，采用的是hadoop方式读取，即按行读取
     * 其次，数据读取时以偏移量作为单位，且不会被重复读取
     *
     * 如            分区计算
     * 1@@ => 012        0 => [0,3] 即1@@2  由于按行读取，所以末尾@@也被读取
     * 2@@ => 345        1 => [3,6] 2@@已经被读取，所以读3
     * 3 => 6            2 => [6,7] 无数据
     * */
    val rdd2 = sc.textFile("file/input.txt", 2)
    //将处理的数据保存成分区文件
    rdd2.saveAsTextFile("file/output")

    //TODO 关闭环境
    sc.stop()
  }
}
