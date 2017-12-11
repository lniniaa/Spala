import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

class SpalaJob(sc: SparkContext) {
  val input = sc.textFile(getClass().getResource("text.txt").toString)
  val words = input.flatMap(line => line.split(" "))
  val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
  counts.collect().sorted.foreach(println)
}

object SpalaJob {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spala").setMaster("local")
    val context = new SparkContext(conf)
    val job = new SpalaJob(context)
    context.stop()
  }
}
