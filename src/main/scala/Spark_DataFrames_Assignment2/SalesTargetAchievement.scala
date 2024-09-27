package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object SalesTargetAchievement {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("SalesTargetAchievement")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val salesTargets = List(
        (1, "John Smith", 15000, 12000),
        (2, "Jane Doe", 9000, 10000),
        (3, "John Doe", 5000, 6000),
        (4, "John Smith", 13000, 13000),
        (5, "Jane Doe", 7000, 7000),
        (6, "John Doe", 8000, 8500)
    ).toDF("sales_id", "sales_rep", "sales_amount", "target_amount")

    //salesTargets.show()
    val salesAchievementStatusDF = salesTargets.withColumn("achievement_status", when(col("sales_amount") >= col("target_amount"), "Above Target")
      .when(col("sales_amount") < col("target_amount"), "Below Target"))

    salesAchievementStatusDF.show()

    salesTargets.filter(col("sales_rep").contains("John")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount
    //for each achievement_status
    salesAchievementStatusDF.groupBy(col("achievement_status")).agg(sum(col("sales_amount")).as("total_sales_amount"),
      avg(col("sales_amount")), min(col("sales_amount")), max(col("sales_amount"))).show()

    println("================ Spark SQL ================")
    salesTargets.createOrReplaceTempView("sales_targets")
    salesAchievementStatusDF.createOrReplaceTempView("sales_achievement_status_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN sales_amount >= target_amount THEN 'Above Target'
        WHEN sales_amount < target_amount THEN 'Below Target'
        END AS achievement_status
        FROM sales_targets
        """).show()

    spark.sql(
      """
        SELECT *
        FROM sales_targets
        WHERE sales_rep like '%John%'
        """).show()

    spark.sql(
      """
        SELECT
        achievement_status,
        sum(sales_amount) as total_sales_amount, avg(sales_amount),
        min(sales_amount), max(sales_amount)
        FROM sales_achievement_status_tab
        GROUP BY achievement_status
        """).show()

  }
}
