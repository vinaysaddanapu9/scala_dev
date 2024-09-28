package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object MarketingCampaignExpenses {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MarketingCampaignExpenses")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val campaignExpenses = List(
      (1, "Summer Blast", 22000, "2024-11-01"),
      (2, "Summer Sale", 12000, "2024-11-05"),
      (3, "Winter Campaign", 8000, "2024-11-10"),
      (4, "Summer Special", 15000, "2024-11-15"),
      (5, "Winter Special", 9000, "2024-11-20"),
      (6, "Summer Promo", 25000, "2024-11-25")
    ).toDF("campaign_id", "campaign_name", "expense_amount", "expense_date")

    //campaignExpenses.show()
    val expenseTypeDF = campaignExpenses.withColumn("expense_type", when(col("expense_amount") > 20000, "High")
      .when((col("expense_amount") >= 10000) and (col("expense_amount") <= 20000), "Medium")
    .when(col("expense_amount") < 10000, "Low"))

    expenseTypeDF.show()

    campaignExpenses.filter(col("campaign_name").startsWith("Summer")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //expense_amount for each expense_type

    expenseTypeDF.groupBy(col("expense_type")).agg(sum(col("expense_amount")), avg(col("expense_amount")),
      min(col("expense_amount")), max(col("expense_amount"))).show()

    println("============= Spark SQL ================")
    campaignExpenses.createOrReplaceTempView("campaign_expenses")
    expenseTypeDF.createOrReplaceTempView("expense_type_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN expense_amount > 20000 THEN 'High'
        WHEN expense_amount BETWEEN 10000 and 20000 THEN 'Medium'
        WHEN expense_amount < 10000 THEN 'Low'
        END AS expense_type
        FROM campaign_expenses
        """).show()

    spark.sql(
      """
        SELECT *
        FROM campaign_expenses
        WHERE campaign_name like 'Summer%'
        """).show()

    spark.sql(
      """
        SELECT
        expense_type,
        sum(expense_amount), avg(expense_amount),
        min(expense_amount), max(expense_amount)
        FROM expense_type_tab
        GROUP BY expense_type
        """).show()
  }
}
