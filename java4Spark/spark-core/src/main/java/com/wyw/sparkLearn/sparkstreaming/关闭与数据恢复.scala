package com.wyw.sparkLearn.sparkstreaming

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 *
 * @author 5yw
 * @date 2022/3/9 16:44
 */
object 关闭与数据恢复 {
  def main(args: Array[String]): Unit = {
    //TODO 数据恢复
    val sc = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkconf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamning")
      val sc = new StreamingContext(sparkconf, Seconds(3))
      sc
    })
    sc.checkpoint("cp")

    //TODO 关闭
    sc.start()
    new Thread(new Runnable {
      override def run(): Unit = {
        val state: StreamingContextState = sc.getState
        if (true) {     //这里应该是任务是否完成的判断，比如数据库是否读取完成
          if (state == StreamingContextState.ACTIVE) {    //只有检测当前sparkstreaming是激活的，才需要执行关闭操作
            sc.stop(stopSparkContext = true, stopGracefully = true)   //当执行关闭操作时，接收操作先关闭，等待已接收数据都处理完成后，整个关闭
            System.exit(0)    //线程关闭
          }
        }
      }
    }
    ).start()
    sc.awaitTermination()
  }
}
