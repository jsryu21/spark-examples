import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PageRank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PageRank Application")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/home/jsryu21/GitHub/spark/data/mllib/pagerank_data.txt")
    val links = lines.map{s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    val ranks = links.mapValues(v => 1.0)
    val contribs = links.join(ranks).values.flatMap{case(urls, rank) =>
      val size = urls.size
      urls.map(url => (url, rank / size))
    }
    val ranks2 = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    ranks2.collect().foreach(println)
    println(ranks2.toDebugString)
  }
}
