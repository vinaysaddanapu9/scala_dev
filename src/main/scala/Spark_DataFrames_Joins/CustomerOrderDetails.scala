package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CustomerOrderDetails {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("CustomerOrderDetails")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val customers = Seq(
      ("C001", "Alice"),
      ("C002", "Bob"),
      ("C003", "Charlie"),
      ("C004", "David")
    ).toDF("customer_id","customer_name")

    customers.show()

    val orders = Seq(
      ("O001", "C001", "P001"),
      ("O002", "C002", "P002"),
      ("O003", "C003", "P003"),
      ("O004", "C001", "P004"),
      ("O005", "C004", "P001")
    ).toDF("order_id", "customer_id", "product_id")

    orders.show()

    val products = Seq(
      ("P001", "Laptop"),
      ("P002", "Mobile"),
      ("P003", "Tablet"),
      ("P004", "Monitor")
    ).toDF("product_id","product_name")

    products.show()

    val condition = orders("customer_id") === customers("customer_id")

    val condition2 = orders("product_id") === products("product_id")

    val joinType = "inner"

    val joined_df = orders.join(customers, condition, joinType)
                          .join(products, condition2, joinType)

    joined_df.show()

  }
}
