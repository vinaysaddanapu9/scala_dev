package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object BookStoreInventory {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("BookStoreInventory")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
     val  bookInventory = List(
        (1, "The Great Gatsby", 150, "2024-01-10"),
        (2, "The Catcher in the Rye", 80, "2024-01-15"),
        (3, "Moby Dick", 200, "2024-01-20"),
        (4, "To Kill a Mockingbird", 30, "2024-02-01"),
        (5, "The Odyssey", 60, "2024-02-10"),
        (6, "War and Peace", 20, "2024-03-01")
     ).toDF("book_id", "book_title", "stock_quantity", "last_updated")

    //bookInventory.show(truncate = false)

    val bookStockLevelDF = bookInventory.withColumn("stock_level", when(col("stock_quantity") > 100, "High")
      .when((col("stock_quantity") >= 50) && (col("stock_quantity") <= 100), "Medium")
      .when(col("stock_quantity") < 50, "Low"))

    bookStockLevelDF.show(truncate = false)

    bookInventory.filter(col("book_title").startsWith("The")).show(truncate = false)

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) stock_quantity
    //for each stock_level

    bookStockLevelDF.groupBy(col("stock_level")).agg(sum(col("stock_quantity")).as("total_sum"), avg(col("stock_quantity")),
    min(col("stock_quantity")), max(col("stock_quantity"))).show()

    println("============== Spark SQL ===================")

    bookInventory.createOrReplaceTempView("book_inventory")
    bookStockLevelDF.createOrReplaceTempView("book_stock_level_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN stock_quantity > 100 THEN 'High'
        WHEN stock_quantity BETWEEN 50 and 100 THEN 'Medium'
        WHEN stock_quantity < 50 THEN 'Low'
        END AS stock_level
        FROM book_inventory
        """).show(truncate = false)

    spark.sql(
      """
        SELECT *
        FROM book_inventory
        WHERE book_title like 'The%'
        """).show(truncate = false)

    spark.sql(
      """
        SELECT
        stock_level,
        sum(stock_quantity) as total_sum, avg(stock_quantity),
        min(stock_quantity), max(stock_quantity)
        FROM
        book_stock_level_tab
        GROUP BY stock_level
        """).show()


  }
}
