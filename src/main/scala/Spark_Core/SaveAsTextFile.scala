package Spark_Core

import org.apache.spark.SparkContext

object SaveAsTextFile {

  def main(args: Array[String]): Unit = {

    val a = (10, 20, 30, 40)
    //print(a._1)
    val sc = new SparkContext("local[4]", "sparkrdd")

    val data = Array(1, 2, 3, 4, 5)
    val input = sc.parallelize(data)
    input.saveAsTextFile("E:/Files/Sep16")
    //val result = input.mean()
    //println("Result: "+ result)

  }
}
