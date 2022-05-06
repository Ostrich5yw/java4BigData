package com.wyw.sparkLearn.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 *
 * @author 5yw
 * @date 2022/3/9 9:55
 */
object 自定义数据源 {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //SparkStreamning需要两个参数
    // 第一个参数表示环境配置,第二个参数表示时间序列长度
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamning")
    val sc = new StreamingContext(sparkconf, Seconds(3))

    //TODO 逻辑操作
    val message = sc.receiverStream(new MyReceiver())
    message.print()
    sc.start()

    //TODO 关闭对象
    //Spark 采集器是长期任务，不能直接关闭
    sc.awaitTermination()
  }
  /**
  自定义数据源
   */
  class MyReceiver() extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flage = true

    //最初启动的时候，调用该方法，作用为：读数据并将数据发送给 Spark
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flage) {
            val message = "采集的数据为:" + new Random().nextInt(10).toString()
            store(message)
            Thread.sleep(300)
          }
        }
      }).start()

    }
    override def onStop(): Unit =  {
      flage = false
    }
  }

}
