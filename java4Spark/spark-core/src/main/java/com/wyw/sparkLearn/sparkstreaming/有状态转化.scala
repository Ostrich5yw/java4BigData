package com.wyw.sparkLearn.sparkstreaming

import com.wyw.sparkLearn.sparkstreaming.自定义数据源.MyReceiver
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @author 5yw
 * @date 2022/3/9 15:25
 */
object 有状态转化 {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //SparkStreamning需要两个参数
    // 第一个参数表示环境配置,第二个参数表示时间序列长度
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamning")
    val sc = new StreamingContext(sparkconf, Seconds(3))

    // 在包内的netcat文件夹运行cmd，并输入nc -lp 端口号  进行socket通信
    //TODO 逻辑操作
    // 无状态wordCount，只会记录以3S为一段的段内wordCount
//    val line = sc.socketTextStream("localhost", 8082) //监听8082端口
//    val value = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
//    value.print()

    //TODO 逻辑操作
    // 有状态wordCount，记录直到目前的总体wordCount
    sc.checkpoint("./cp")
    val line2 = sc.socketTextStream("localhost", 8083) //监听8082端口
    val word2 = line2.flatMap(_.split(" ")).map((_, 1))
    /**
     * 根据key对数据的状态进行更新
     * 传递两个参数
     * 第一个：表示相同的key的value数据
     * 第二个：表示缓冲区相同key的value数据,因为初始缓冲区可能没有数据，所以定义为Option[]
     *
     * 在wordCount中，加入现在缓冲区已经有(word, 3) (hello, 4)
     * 第一个参数表示新接收到的RDD的value(1),第二个参数表示为(3) (4) (根据不同key进行对应计算)
     * */
    val value2 = word2.updateStateByKey(
      (seq: Seq[Int], buf:Option[Int]) => {
        val newCount = buf.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )
    value2.print()


    sc.start()

    //TODO 关闭对象
    //Spark 采集器是长期任务，不能直接关闭
    sc.awaitTermination()
  }
}
