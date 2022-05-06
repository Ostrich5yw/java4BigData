package com.wyw.sparkLearn.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 * @author 5yw
 * @date 2022/3/8 16:30
 */
object 连接Hive {
  def main(args: Array[String]): Unit = {

    //TODO 创建SparkSQL基本环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    import spark.implicits._ //导入隐式转换，即$"列名"
    //TODO 逻辑操作
    spark.sql("show tables").show

    //TODO 关闭连接
    spark.stop()
  }
}
