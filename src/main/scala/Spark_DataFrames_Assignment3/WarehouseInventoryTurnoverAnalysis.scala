package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, datediff, lag, sum}

object WarehouseInventoryTurnoverAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("WarehouseInventoryTurnoverAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val inventoryTurnover = List(
        ("I001", "W001", "Widget A", "2024-01-01", "2024-01-20", 100, 120),
        ("I002", "W002", "Widget B", "2024-01-02", "2024-02-01", 200, 180),
        ("I003", "W001", "Widget A", "2024-01-15", "2024-02-05", 150, 160),
        ("I004", "W003", "Widget C", "2024-02-01", "2024-02-15", 80, 90),
        ("I005", "W002", "Widget B", "2024-02-10", "2024-02-25", 200, 220),
        ("I006", "W001", "Widget A", "2024-01-25", "2024-02-05", 120, 140),
        ("I007", "W003", "Widget C", "2024-02-10", "2024-02-20", 90, 100),
        ("I008", "W001", "Widget A", "2024-02-01", "2024-02-12", 150, 180),
        ("I009", "W002", "Widget B", "2024-02-05", "2024-02-20", 130, 140),
        ("I010", "W003", "Widget C", "2024-02-15", "2024-02-28", 110, 120)
    ).toDF("inventory_id","warehouse_id","product_name","stock_in_date","stock_out_date",
      "stock_in_quantity","stock_out_quantity")

    //inventoryTurnover.show()

    val turnOverDF = inventoryTurnover.withColumn("turnover_duration",
      datediff(col("stock_out_date"),col("stock_in_date")))

    turnOverDF.show()

    turnOverDF.filter(col("turnover_duration") < 30
      && (col("stock_out_quantity") > col("stock_in_quantity"))).show()

    turnOverDF.groupBy(col("warehouse_id"), col("product_name"))
      .agg(sum(col("stock_out_quantity")), count(col("turnover_duration")),
        avg(col("turnover_duration"))).show()

    //Use the lag function to track changes in stock_in_quantity for each product over time.
    val window = Window.partitionBy("product_name").orderBy("stock_in_date")
    val prevStockInQuantityDF = turnOverDF.withColumn("prev_stock_in_quantity", lag(col("stock_in_quantity"), 1).over(window))
    prevStockInQuantityDF.select(col("*"), (col("stock_in_quantity") - col("prev_stock_in_quantity"))
      .as("diff_stock_in_quantity")).show()

    println("=================== Spark SQL ========================")
    inventoryTurnover.createOrReplaceTempView("inventory_turnover")
    turnOverDF.createOrReplaceTempView("turn_over_tab")

    spark.sql(
      """
        SELECT *,
        datediff(stock_out_date, stock_in_date) AS turnover_duration
        FROM inventory_turnover
        """).show()

    spark.sql(
      """
        SELECT *
        FROM turn_over_tab
        WHERE turnover_duration < 30
        and stock_out_quantity > stock_in_quantity
        """).show()

    spark.sql(
      """
        SELECT
        warehouse_id, product_name,
        sum(stock_out_quantity), count(turnover_duration),
        avg(turnover_duration)
        FROM turn_over_tab
        GROUP BY warehouse_id, product_name
        """).show()

    spark.sql(
      """
        SELECT *,
        stock_in_quantity -
        LAG(stock_in_quantity, 1) OVER(partition by product_name order by stock_in_date)
        AS diff_stock_in_quantity
        FROM turn_over_tab
        """).show()

  }
}
