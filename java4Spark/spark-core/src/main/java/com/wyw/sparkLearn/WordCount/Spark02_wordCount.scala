import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author 5yw
 * @date 2021/10/27 9:40
 */
object Spark02_wordCount {  //(hello,hello) 变为 ((hello,1),(hello,1))
  def main(args: Array[String]):Unit = {
    // TODO 建立与Spark框架的连接
    val sConfig = new SparkConf().setMaster("local").setAppName("WordCount")
    val sC = new SparkContext(sConfig)
    // TODO 执行业务操作
    // 1. 读取一行一行数据
    var lines: RDD[String] = sC.textFile("spark-core/data")
    // 2. 将行拆分，切割出一个一个单词(扁平化)
    var words: RDD[String] = lines.flatMap(_.split(" "))
    var wordToOne = words.map{
      word => (word, 1)
    }
    // 3. 将单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
    // 4. 对分组数据进行转换
    // (hello, hello, hello) ——> (hello, 3)
    val wordToCount = wordGroup.map{
      case (word, list) => {
        list.reduce(
          (t1, t2) => (t1._1, t1._2 + t2._2)
        )
        /**
         * reduce作用((hello,1),(hello,1),(hello,1))
         * ——> ((hello,1),(hello,1))
         * ——> ((hello,2),(hello,1))
         * */
      }
    }
    // 5. 将转换结果打印出来
    var array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    // TODO 关闭连接
  }
}
