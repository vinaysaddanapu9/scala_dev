package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object SalesPerformanceAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

   val spark = SparkSession.builder()
     .appName("SalesPerformanceAnalysis")
     .master("local[*]")
     .getOrCreate()

    import spark.implicits._
    val salesPerformance = List(
      (1, "North-West", 12000, "2024-01-10"),
      (2, "South-East", 6000, "2024-01-15"),
      (3, "East-Central", 4000, "2024-02-20"),
      (4, "West", 15000, "2024-02-25"),
      (5, "North-East", 3000, "2024-03-05"),
      (6, "South-West", 7000, "2024-03-12")
    ).toDF("sales_id", "region", "sales_amount", "sales_date")

    //salesPerformance.show()
    val performanceCategory = salesPerformance.withColumn("sales_performance", when(col("sales_amount") > 10000, "Excellent")
      .when((col("sales_amount") >= 5000) && (col("sales_amount") <= 10000), "Good")
      .when(col("sales_amount") < 5000, "Average"))

    performanceCategory.show()

    salesPerformance.filter(col("region").endsWith("West")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount
    //for each performance category
    performanceCategory.groupBy(col("sales_performance")).agg(sum(col("sales_amount")), avg(col("sales_amount"))
    , max(col("sales_amount")), min(col("sales_amount"))).show()

    println("================= Spark SQL ================")
    salesPerformance.createOrReplaceTempView("sales_performance")
    performanceCategory.createOrReplaceTempView("performance_category")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN sales_amount > 10000 THEN 'Excellent'
        WHEN sales_amount BETWEEN 5000 and 10000 THEN 'Good'
        WHEN sales_amount < 5000 THEN 'Average'
        END AS sales_performance
        FROM sales_performance
        """).show()

    spark.sql(
      """
        SELECT *
        FROM sales_performance
        WHERE region like '%West'
        """).show()

      spark.sql(
        """
          SELECT
          sales_performance,
          sum(sales_amount), avg(sales_amount),
          max(sales_amount), min(sales_amount)
          FROM performance_category
          GROUP BY sales_performance
          """).show()

  }
}
