import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 5yw
 * @date 2021/10/27 8:33
 */
object Spark01_wordCount { //通过list.size直接获得统计个数
  def main(args: Array[String]):Unit = {
    // TODO 建立与Spark框架的连接
    val sConfig = new SparkConf().setMaster("local").setAppName("WordCount")
    val sC = new SparkContext(sConfig)
    // TODO 执行业务操作
    // 1. 读取一行一行数据
    var lines: RDD[String] = sC.textFile("data/wordCount.txt")
    // 2. 将行拆分，切割出一个一个单词(扁平化)
    var words: RDD[String] = lines.flatMap(_.split(" "))
    // 3. 将单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word=>word)
    // 4. 对分组数据进行转换
    // (hello, hello, hello) ——> (hello, 3)
    val wordToCount = wordGroup.map{
      case (word, list) => {
        (word, list.size)
      }
    }
    // 5. 将转换结果打印出来
    var array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    // TODO 关闭连接
  }
}
