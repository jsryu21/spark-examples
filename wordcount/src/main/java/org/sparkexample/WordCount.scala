import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val testFilePath = // path
    val outputFilePath = // path
    val conf = new SparkConf().setAppName("WordCount Application")
    val sc = new SparkContext(conf)
    val file = sc.textFile(/**/)
    val counts = file.flatMap(/* split */)
      .map(/* word to pair(word, 1)*/)
      .reduceByKey(/**/)
    counts.saveAsTextFile(/**/)
  }
}
