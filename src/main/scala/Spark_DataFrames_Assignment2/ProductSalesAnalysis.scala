package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object ProductSalesAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("ProductSalesAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val productSales = List(
        (1, "Tech Gadget", 6000, "2025-02-01"),
        (2, "Tech Widget", 3000, "2025-02-05"),
        (3, "Home Gadget", 1500, "2025-02-10"),
        (4, "Tech Tool", 5000, "2025-02-15"),
        (5, "Tech Device", 7000, "2025-02-20"),
        (6, "Office Gadget", 1800, "2025-02-25")
    ).toDF("sale_id", "product_name","sale_amount","sale_date")

    //productSales.show()
    val salesCategoryDF = productSales.withColumn("sales_category", when(col("sale_amount") > 5000, "High")
      .when((col("sale_amount") >= 2000) and (col("sale_amount") <= 5000), "Medium")
      .when(col("sale_amount") < 2000, "Low"))

    salesCategoryDF.show()

    productSales.filter(col("product_name").startsWith("Tech")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) sale_amount
    //for each sales_category

    salesCategoryDF.groupBy("sales_category").agg(sum(col("sale_amount")), avg(col("sale_amount")),
      min(col("sale_amount")), max(col("sale_amount"))).show()

    println("================== Spark SQL =======================")
    productSales.createOrReplaceTempView("product_sales")
    salesCategoryDF.createOrReplaceTempView("sales_category_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN sale_amount > 5000 THEN 'High'
        WHEN sale_amount BETWEEN 2000 and 5000 THEN 'Medium'
        WHEN sale_amount < 2000 THEN 'Low'
        END AS sales_category
        FROM product_sales
        """).show()

    spark.sql(
      """
        SELECT *
        FROM product_sales
        WHERE product_name like 'Tech%'
        """).show()

    spark.sql(
      """
        SELECT
        sales_category,
        sum(sale_amount), avg(sale_amount),
        min(sale_amount), max(sale_amount)
        FROM sales_category_tab
        GROUP BY sales_category
        """).show()

  }
}
