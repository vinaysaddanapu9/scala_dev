package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object OnlineStoreInventoryManagement {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("OnlineStoreInventoryManagement")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val storeInventory = List(
        (1, "Widget Lite", 15, "2024-01-10"),
        (2, "Gadget", 60, "2024-01-15"),
        (3, "Light Lite", 25, "2024-02-20"),
        (4, "Appliance", 5, "2024-02-25"),
        (5, "Widget Pro", 70, "2024-03-05"),
        (6, "Light Pro", 45, "2024-03-12")
    ).toDF("product_id", "product_name", "quantity_in_stock", "last_restocked")

    //storeInventory.show()
    val stockStatusDF = storeInventory.withColumn("stock_status", when(col("quantity_in_stock") < 20, "Critical")
      .when((col("quantity_in_stock") >= 20) and (col("quantity_in_stock") < 50), "Low")
      .when(col("quantity_in_stock") >= 50, "Sufficient"))

    stockStatusDF.show()

    storeInventory.filter(col("product_name").endsWith("Lite")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //quantity_in_stock for each stock_status
    stockStatusDF.groupBy(col("stock_status")).agg(sum(col("quantity_in_stock")), avg(col("quantity_in_stock")),
      min(col("quantity_in_stock")), max(col("quantity_in_stock"))).show()

    println("============ Spark SQL ==================")
    storeInventory.createOrReplaceTempView("store_inventory")
    stockStatusDF.createOrReplaceTempView("stock_status_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN quantity_in_stock < 20 THEN 'Critical'
        WHEN quantity_in_stock BETWEEN 20 and 50 THEN 'Low'
        WHEN quantity_in_stock >= 50 THEN 'Sufficient'
        END AS stock_status
        FROM store_inventory
        """).show()

    spark.sql(
      """
        SELECT *
        FROM store_inventory
        WHERE product_name like '%Lite'
        """).show()

    spark.sql(
      """
        SELECT
        stock_status,
        sum(quantity_in_stock) as total_sum_quantity_in_stock, avg(quantity_in_stock),
        min(quantity_in_stock), max(quantity_in_stock)
        FROM stock_status_tab
        GROUP BY stock_status
        """).show()



  }
}
