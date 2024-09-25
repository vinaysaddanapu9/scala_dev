package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object ProductInventoryAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

   val spark = SparkSession.builder()
              .appName("ProductInventoryAnalysis")
              .master("local[*]")
              .getOrCreate()

    import spark.implicits._
    val inventory = List(
      (1, "Pro Widget", 30, "2024-01-10"),
      (2, "Pro Device", 120, "2024-01-15"),
      (3, "Standard", 200, "2024-01-20"),
      (4, "Pro Gadget", 40, "2024-02-01"),
      (5, "Standard", 60, "2024-02-10"),
      (6, "Pro Device", 90, "2024-03-01")
    ).toDF("product_id", "product_name", "stock", "last_restocked")

    //inventory.show()
    val inventory_status = inventory.withColumn("stock_status", when(col("stock") < 50, "Low")
  .when((col("stock") >= 50) && (col("stock") <= 150), "Medium").when(col("stock") > 150, "High"))

    inventory_status.show()

    //Filter products where product_name contains 'Pro'.
    inventory.filter(col("product_name").contains("Pro")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) stock for each stock status
    inventory_status.groupBy(col("stock_status")).agg(sum(col("stock")).as("total_sum"), avg(col("stock")),
      min(col("stock")), max(col("stock"))).show()

    println("=========== Spark SQL =================")

    inventory.createOrReplaceTempView("inventory")
    inventory_status.createOrReplaceTempView("inventory_status")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN stock < 50 THEN 'Low'
        WHEN stock BETWEEN 50 and 150 THEN 'Medium'
        WHEN stock > 150 THEN 'High'
        END AS  stock_status
        FROM inventory
        """).show()

    spark.sql(
      """
        SELECT *
        FROM inventory
        WHERE product_name like '%Pro%'
        """).show()

    spark.sql(
      """
        SELECT
        stock_status,
        sum(stock), avg(stock),
        min(stock), max(stock)
        FROM inventory_status
        GROUP BY stock_status
        """).show()

  }
}
