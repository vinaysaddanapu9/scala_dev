package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object ProductReturnAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("ProductReturnAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val  productReturns = List(
        (1, "Widget Pro", 6000, "2024-05-01"),
        (2, "Gadget Pro", 3000, "2024-05-05"),
        (3, "Widget Max", 1500, "2024-05-10"),
        (4, "Gadget Max", 2500, "2024-05-15"),
        (5, "Widget Pro", 7000, "2024-05-20"),
        (6, "Gadget Max", 1000, "2024-05-25")
    ).toDF("return_id", "product_name", "return_amount", "return_date")

    //productReturns.show()
    val returnStatusDF = productReturns.withColumn("return_status", when(col("return_amount") > 5000, "High")
      .when((col("return_amount") >= 2000) and (col("return_amount") <= 5000), "Medium")
      .when(col("return_amount") < 2000, "Low"))

    returnStatusDF.show()

    productReturns.filter(col("product_name").endsWith("Pro")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //return_amount for each return_status
    returnStatusDF.groupBy(col("return_status")).agg(sum(col("return_amount")), avg(col("return_amount")),
      min(col("return_amount")), max(col("return_amount"))).show()

    println("================= Spark SQL =================")
    productReturns.createOrReplaceTempView("product_returns")
    returnStatusDF.createOrReplaceTempView("return_status_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN return_amount > 5000 THEN 'High'
        WHEN return_amount BETWEEN 2000 and 5000 THEN 'Medium'
        WHEN return_amount < 2000 THEN 'Low'
        END AS return_status
        FROM product_returns
        """).show()

    spark.sql(
      """
        SELECT *
        FROM product_returns
        WHERE product_name like '%Pro'
        """).show()

    spark.sql(
      """
        SELECT
        return_status,
        sum(return_amount), avg(return_amount),
        min(return_amount), max(return_amount)
        FROM return_status_tab
        GROUP BY return_status
        """).show()

  }
}
