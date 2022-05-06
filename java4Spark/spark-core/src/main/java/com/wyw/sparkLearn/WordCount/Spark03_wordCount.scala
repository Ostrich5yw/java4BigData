import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2021/10/27 10:19
 */
object Spark03_wordCount {  //前两种方式都是通过scala自带方法进行mapreduce，这里采用Spark封装方法
  def main(args: Array[String]):Unit = {
    // TODO 建立与Spark框架的连接
    val sConfig = new SparkConf().setMaster("local").setAppName("WordCount")
    val sC = new SparkContext(sConfig)
    // TODO 执行业务操作
    // 1. 读取一行一行数据
    var lines: RDD[String] = sC.textFile("data/wordCount.txt")
    // 2. 将行拆分，切割出一个一个单词(扁平化)
    var words: RDD[String] = lines.flatMap(_.split(" "))
    var wordToOne = words.map{
      word => (word, 1)
    }
    // 3. 将单词进行分组，便于统计
    /**
     * reduceByKey 对相同的key进行value聚合
     * 1, 1, 1 ==> 1 + 1, 1 ==> 2 + 1
     * */
    val wordToCount = wordToOne.reduceByKey((t1, t2) => t1 + t2)
    // 5. 将转换结果打印出来
    var array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    // TODO 关闭连接
  }
}
