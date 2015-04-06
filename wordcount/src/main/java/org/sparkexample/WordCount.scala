import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val testFilePath = "/home/jsryu21/GitHub/spark-examples/wordcount/src/test/resources/loremipsum.txt" // Should be some file on your system
    val outputFilePath = "/home/jsryu21/GitHub/spark-examples/wordcount/wc_out/"
    val conf = new SparkConf().setAppName("WordCount Application")
    val sc = new SparkContext(conf)
    val file = sc.textFile(testFilePath)
    val counts = file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(outputFilePath)
  }
}