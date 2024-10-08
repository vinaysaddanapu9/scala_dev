package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

object ProductSalesAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("ProductSalesAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val products = Seq(
      ("P001", "Laptop"),
      ("P002", "Mobile"),
      ("P003", "Tablet"),
      ("P004", "Monitor"),
      ("P005", "Keyboard")
    ).toDF("product_id", "product_name")

    val sales = Seq(
      ("S001", "P001", 500.0),
      ("S002", "P002", 300.0),
      ("S003", "P001", 700.0),
      ("S004", "P003", 200.0),
      ("S005", "P002", 400.0),
      ("S006", "P004", 600.0),
      ("S007", "P005", 150.0)
    ).toDF("sale_id", "product_id", "amount")

    val condition = products("product_id") === sales("product_id")

    val joinType = "inner"

    val joined_df = products.join(sales, condition, joinType)

    val aggregatedDF = joined_df.groupBy(products.col("product_id")).agg(sum(col("amount")).as("total_amount"))
    aggregatedDF.show()

    println("============== Spark SQL =================")
    products.createOrReplaceTempView("products")
    sales.createOrReplaceTempView("sales")

    spark.sql(
      """
        SELECT
        p.product_id,
        sum(amount) as total_amount
        FROM products p
        JOIN sales s ON
        p.product_id = s.product_id
        GROUP BY p.product_id
        """).show()

  }
}
