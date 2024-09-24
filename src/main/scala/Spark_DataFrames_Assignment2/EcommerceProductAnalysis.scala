package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object EcommerceProductAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("EcommerceProductAnalysisExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val products = List(
      (1, "Smartphone", 700, "Electronics"),
      (2, "TV", 1200, "Electronics"),
      (3, "Shoes", 150, "Apparel"),
      (4, "Socks", 25, "Apparel"),
      (5, "Laptop", 800, "Electronics"),
      (6, "Jacket", 200, "Apparel")
    ).toDF("product_id","product_name","price","category")

    products.withColumn("price_category", when(col("price") > 500, "Expensive")
      .when((col("price") >= 200) && (col("price") <= 500), "Moderate").when(col("price") < 200, "Cheap")).show()

    //Filter products whose product_name starts with "S"
    products.filter(col("product_name").startsWith("S")).show()

    //Filter products whose product_name ends with 's'.
    products.filter(col("product_name").endsWith("s")).show()

    //Calculate the total price (sum), average price, maximum price, and minimum price for each category
    products.groupBy(col("category")).agg(sum(col("price")).as("total price"),
      avg(col("price")), max(col("price")), min(col("price"))).show()

    products.createOrReplaceTempView("products")

    println("======= SQL ========")

    spark.sql(
      """
        SELECT
        *,
        CASE
        WHEN price > 500 THEN 'Expensive'
        WHEN price BETWEEN 200 and 500 THEN 'Moderate'
        WHEN price < 200 THEN 'Cheap'
        END AS
        price_category
        FROM products
        """).show()

    // product name starts with S
    spark.sql(
      """
        SELECT * FROM
        products where product_name like 'S%'
        """).show()

    // product name ends with s
    spark.sql(
      """
        SELECT * FROM
        products where product_name like '%s'
        """).show()

    //Calculate the total price (sum), average price, maximum price, and minimum price for each category
    spark.sql(
      """
        SELECT category,
        sum(price) as total_price, avg(price),
        max(price), min(price)
        FROM products
        GROUP BY category
        """).show()

    //products.show()
  }

}
