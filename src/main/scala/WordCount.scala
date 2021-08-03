import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/home/jin/Desktop/SparkLearning/src/main/resources/helloSpark.txt")

    val lines = input.flatMap(lines => lines.split(" "))
    val count = lines.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }

//    val output = count.saveAsTextFile("/home/jin/Desktop/SparkLearning/src/main/resources/helloSparkRes.txt")
    count.foreach(println)
  }

}
