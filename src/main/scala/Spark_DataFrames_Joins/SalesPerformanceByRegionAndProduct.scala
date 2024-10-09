package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

object SalesPerformanceByRegionAndProduct {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("SalesPerformanceByRegionAndProduct")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val salespersons = Seq(
      ("SP001", "Alice"),
      ("SP002", "Bob"),
      ("SP003", "Charlie")
    ).toDF("salesperson_id", "salesperson_name")

    val products = Seq(
      ("P001", "Laptop"),
      ("P002", "Mobile"),
      ("P003", "Tablet"),
      ("P004", "Monitor")
    ).toDF("product_id", "product_name")

    val regions = Seq(
      ("R001", "North"),
      ("R002", "South"),
      ("R003", "East"),
      ("R004", "West")
    ).toDF("region_id", "region_name")

    val sales = Seq(
      ("S001", "P001", "R001", "SP001", 800.0),
      ("S002", "P002", "R002", "SP001", 400.0),
      ("S003", "P003", "R003", "SP002", 300.0),
      ("S004", "P004", "R001", "SP002", 200.0),
      ("S005", "P001", "R004", "SP003", 800.0),
      ("S006", "P002", "R003", "SP003", 400.0)
    ).toDF("sale_id", "product_id", "region_id", "salesperson_id", "amount")


    val condition = sales("region_id") === regions("region_id")

    val condition2 = sales("product_id") === products("product_id")

    val condition3 = sales("salesperson_id") === salespersons("salesperson_id")

    val joinType = "inner"

    val joined_df = sales.join(regions, condition, joinType)
      .join(products, condition2, joinType)
      .join(salespersons, condition3, joinType)

      joined_df.show()

    val aggregated_df = joined_df.groupBy(col("product_name"), col("region_name"), col("salesperson_name"))
      .agg(sum(col("amount")).as("total_sales"))
    aggregated_df.show()

    println("================ Spark SQL ===================")
    salespersons.createOrReplaceTempView("salespersons")
    products.createOrReplaceTempView("products")
    regions.createOrReplaceTempView("regions")
    sales.createOrReplaceTempView("sales")


    spark.sql(
      """
        SELECT *
        FROM sales s
        JOIN regions r ON
        s.region_id = r.region_id
        JOIN products p ON
        s.product_id = p.product_id
        JOIN salespersons sp ON
        s.salesperson_id = sp.salesperson_id
        """).show()





  }
}
