package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lag, lit, min, sum, when}

object SalesTargetAchievementAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("SalesTargetAchievementAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val salesTarget = List(
        ("S001", 15000, 12000, "2023-12-10", "North", "Electronics Accessories"),
        ("S002", 8000, 9000, "2023-12-11", "South", "Home Appliances"),
        ("S003", 20000, 18000, "2023-12-12", "East", "Electronics Gadgets"),
        ("S004", 10000, 15000, "2023-12-13", "West", "Electronics Accessories"),
        ("S005", 18000, 15000, "2023-12-14", "North", "Furniture Accessories")
    ).toDF("salesperson_id","sales_amount","target_amount","sale_date","region","product_category")

    salesTarget.show(truncate=false)

    val targetAchievedDF = salesTarget.withColumn("target_achieved",
      when(col("sales_amount") >= col("target_amount"), lit("Yes")).otherwise(lit("No")))

    targetAchievedDF.show(truncate=false)

    salesTarget.filter(col("product_category").startsWith("Electronics")
      && col("product_category").endsWith("Accessories")).show(truncate=false)

    targetAchievedDF.groupBy(col("region"),col("product_category"))
      .agg(sum(col("sales_amount")),
        min(col("sales_amount")),
        count(when(col("target_achieved") === "Yes", 1)).as("count_target_achieved"))
      .show(truncate=false)

    //Use the lag function to compare each salesperson's current sales with their previous sales
    val window = Window.orderBy("sale_date")
    salesTarget.select(col("*"),
      lag(col("sales_amount"),1).over(window).as("previous_sales_amount")).show(truncate=false)

    println("=================== Spark SQL ========================")
    salesTarget.createOrReplaceTempView("sales_targets")
    targetAchievedDF.createOrReplaceTempView("target_achieved_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN sales_amount >= target_amount THEN 'Yes' ELSE 'No'
        END as target_achieved
        FROM sales_targets
        """).show()

    spark.sql(
      """
        SELECT *
        FROM sales_targets
        WHERE product_category like 'Electronics%'
        AND product_category like '%Accessories'
        """).show(truncate = false)

    spark.sql(
      """
        SELECT
        region, product_category,
        sum(sales_amount),
        min(sales_amount),
        COUNT(CASE WHEN target_achieved = 'Yes' THEN 1 END) as count_target_achieved
        FROM target_achieved_tab
        GROUP BY region, product_category
        """).show(truncate = false)

    spark.sql(
      """
        SELECT *,
        LAG(sales_amount, 1) OVER(order by sale_date) AS previous_sales_amount
        FROM sales_targets
        """).show(truncate = false)

  }
}
