package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, max}

object MaximumRevenue {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Practice2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val salesData = Seq(
      ("Product1", "Category1", 100),
      ("Product2", "Category2", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category3", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category3", 180)
    ).toDF("Product", "Category","Revenue")

    salesData.show()

    //Finding the maximum revenue for each product category and the corresponding product.
    val window = Window.partitionBy("Category").orderBy(asc("Category"))

    salesData.withColumn("Maximum_Revenue", max(col("Revenue")).over(window)).show()


    val window2 = Window.partitionBy("Product")
    salesData.withColumn("Max_Revenue_product", max(col("Revenue")).over(window2)).show()

  }
}
