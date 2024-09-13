package Spark_Core

import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {

    val filePath : String = "E:/Files/data5.txt"
    val sc = new SparkContext("local[4]", "SparkRDD")
    val rdd1 = sc.textFile(filePath)
    val rdd2 = rdd1.flatMap(x => x.split(" "))
    val rdd3 = rdd2.map(x => (x,1))
    val rdd4 = rdd3.reduceByKey((x,y) => x+y)
    rdd4.collect().foreach(println)

  }
}
