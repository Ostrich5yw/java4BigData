package com.wyw.sparkLearn.sparksql

import java.lang

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
 *
 * @author 5yw
 * @date 2022/3/7 20:41
 */
object basic {
  def main(args: Array[String]): Unit = {
    /**
     *
     *
     *
     *
     *
     *
     *
     *
     * 需要注释掉java4BigData中的hadoop依赖才可以正常运行
     *
     *
     * */
    //TODO 创建SparkSQL基本环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._    //导入隐式转换，即$"列名"
    //TODO 逻辑操作
    val df = spark.read.json("D:\\AllProject\\JavaProjects\\java4BigData\\file\\user.json")
    df.show()

    val ds = Seq(1,2,3,4).toDS()
    ds.show()

    //三者转化
    val rdd = spark.sparkContext.makeRDD(List(("张三", 13), ("李四", 14)))
    val df2 = rdd.toDF("name", "age")
    val ds2 = df.as[user]
    //TODO 断开连接
    spark.close()
  }
  case class user(
    name: String,
    age: Int
  )
}
