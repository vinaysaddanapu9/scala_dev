package Spark_Core

import org.apache.spark.SparkContext

object IpAddressWordCount {

  def main (args: Array[String]): Unit = {
    val sc = new SparkContext("local[4]", "IPAddress")
    val rdd = sc.textFile("E:/Files/ip_addresses.txt")
    val rdd2 = rdd.flatMap(x => x.split(" "))
    val rdd3 = rdd2.map(x => (x,1))
    val rdd4 = rdd3.reduceByKey((x,y) => x+y)
    rdd4.collect().foreach(println)

    // For Lineage Graph
    print(rdd3.toDebugString)
  }
}
