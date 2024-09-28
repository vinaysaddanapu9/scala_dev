package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.expressions.Window

object WindowFunctions {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val sparkconf=new SparkConf()
    sparkconf.set("spark.app.Name","karthik")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark=SparkSession.builder()
      .config(sparkconf)
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._
    val salesData = Seq(
      ("Product1", "Category1", 100),
      ("Product2", "Category2", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category3", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category3", 180),
      ("Product1", "Category2", 100),
      ("Product2", "Category1", 200),
      ("Product3", "Category2", 150),
      ("Product4", "Category4", 300),
      ("Product5", "Category1", 250),
      ("Product6", "Category4", 180)
    ).toDF("Product", "Category", "Revenue")



    val windowSpec = Window.partitionBy("Product").orderBy("Category")

    val runningTotal = salesData.withColumn("RunningTotal", sum("Revenue").over(windowSpec))
    runningTotal.show()

  }
}
