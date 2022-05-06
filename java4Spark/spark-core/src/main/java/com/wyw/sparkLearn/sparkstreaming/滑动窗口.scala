package com.wyw.sparkLearn.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @author 5yw
 * @date 2022/3/9 16:09
 */
object 滑动窗口 {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //SparkStreamning需要两个参数
    // 第一个参数表示环境配置,第二个参数表示时间序列长度
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamning")
    val sc = new StreamingContext(sparkconf, Seconds(3))

    //TODO 逻辑操作
    val line2 = sc.socketTextStream("localhost", 8083) //监听8082端口
    val word2 = line2.flatMap(_.split(" ")).map((_, 1))
    //窗口大小必须是采集周期的整数倍
    val window = word2.window(Seconds(6), Seconds(3))
    window.reduceByKey(_+_).print()


    sc.start()

    //TODO 关闭对象
    //Spark 采集器是长期任务，不能直接关闭
    sc.awaitTermination()
  }
}
