package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lead, least, max, month}

object RetailDiscountAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("RetailDiscountAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val retailDiscountData = List(
      ("D001", "P001", "Leather Bag", "2023-10-01", 25, "Store A"),
      ("D002", "P002", "Backpack", "2023-11-05", 15, "Store B"),
      ("D003", "P003", "Handbag", "2023-12-10", 30, "Store A"),
      ("D004", "P004", "Laptop Bag", "2023-12-15", 20, "Store C"),
      ("D005", "P005", "Messenger Bag", "2024-01-01", 35, "Store A"),
      ("D006", "P006", "Travel Bag", "2024-01-10", 40, "Store B"),
      ("D007", "P007", "Sports Bag", "2024-02-05", 28, "Store A"),
      ("D008", "P008", "Tote Bag", "2024-01-20", 22, "Store C")
    ).toDF("discount_id", "product_id", "product_name", "discount_date", "discount_percentage", "store")

    retailDiscountData.show()

    val retailDiscountMonthDF = retailDiscountData.withColumn("discount_month", month(col("discount_date")))

    retailDiscountMonthDF.show()

    retailDiscountData.filter(col("discount_percentage") > 20 && col("product_name").endsWith("Bag")).show()

    retailDiscountMonthDF.groupBy(col("store"), col("discount_month"))
      .agg(avg(col("discount_percentage")).as("avg_discount"), max(col("discount_percentage")).as("max_discount"),
        count(col("discount_id")).as("count_discount")).show()

    //Use the lead function to predict the next discount percentage for each product.
    val window = Window.partitionBy("product_name").orderBy("discount_date")
    retailDiscountData.select(col("*"),
      lead(col("discount_percentage"), 1).over(window).as("next_discount_percentage")).show()

    println("===================== Spark SQL ==============================")
    retailDiscountData.createOrReplaceTempView("retail_discounts")
    retailDiscountMonthDF.createOrReplaceTempView("retail_discount_month_tab")

    spark.sql(
      """
        SELECT *,
        EXTRACT(month FROM discount_date) AS discount_month
        FROM retail_discounts
        """).show()

    spark.sql(
      """
        SELECT *
        FROM retail_discounts
        WHERE discount_percentage > 20 and product_name like '%Bag'
        """).show()

    spark.sql(
      """
        SELECT
        store, discount_month,
        avg(discount_percentage) AS avg_discount,
        max(discount_percentage) AS max_discount,
        count(discount_id) AS count_discount
        FROM retail_discount_month_tab
        GROUP BY store, discount_month
        """).show()

    spark.sql(
      """
        SELECT *,
        LEAD(discount_percentage, 1) OVER(partition by product_name order by discount_date) as next_disc_percentage
        FROM retail_discount_month_tab
        """).show()

  }
}
