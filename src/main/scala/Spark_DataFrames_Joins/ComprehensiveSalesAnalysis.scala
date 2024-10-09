package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ComprehensiveSalesAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("ComprehensiveSalesAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val customers = Seq(
      ("C001", "Alice", "R001"),
      ("C002", "Bob", "R002"),
      ("C003", "Charlie", "R003"),
      ("C004", "David", "R001"),
      ("C005", "Eve", "R004")
    ).toDF("customer_id", "customer_name", "region_id")

    val orders = Seq(
      ("O001", "C001", "P001"),
      ("O002", "C002", "P002"),
      ("O003", "C001", "P003"),
      ("O004", "C003", "P001"),
      ("O005", "C004", "P004"),
      ("O006", "C005", "P002"),
      ("O007", "C005", "P003")
    ).toDF("order_id", "customer_id", "product_id")

    val products = Seq(
      ("P001", "Laptop", 800.0),
      ("P002", "Mobile", 400.0),
      ("P003", "Tablet", 300.0),
      ("P004", "Monitor", 200.0)
    ).toDF("product_id", "product_name", "price")

    val regions = Seq(
      ("R001", "North"),
      ("R002", "South"),
      ("R003", "East"),
      ("R004", "West")
    ).toDF("region_id", "region_name")

    val condition = orders("product_id") === products("product_id")

    val condition2 = orders("customer_id") === customers("customer_id")

    val condition3 = customers("region_id") === regions("region_id")

    val joinType = "inner"

     val joined_df = orders.join(products, condition, joinType).join(customers, condition2, joinType)
      .join(regions, condition3, joinType).drop(products("product_id")).drop(customers("region_id"))


    joined_df.show()

    println("==================== Spark SQL ========================")
     customers.createOrReplaceTempView("customers")
     orders.createOrReplaceTempView("orders")
     products.createOrReplaceTempView("products")
     regions.createOrReplaceTempView("regions")

    spark.sql(
      """
        SELECT *
        FROM orders o
        JOIN products p ON
        o.product_id = p.product_id
        JOIN customers c ON
        o.customer_id = c.customer_id
        JOIN regions r ON
        c.region_id = r.region_id
        """).show()

  }
}
