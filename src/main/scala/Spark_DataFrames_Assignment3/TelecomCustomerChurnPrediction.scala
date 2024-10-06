package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, datediff, lead, sum, to_date, when}

object TelecomCustomerChurnPrediction {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("TelecomCustomerChurnPrediction")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val customerChurn = List(
        ("C001", "CU001", "Premium", "2023-01-01", "2024-01-01", 120, true),
        ("C002", "CU002", "Standard", "2023-02-01", "2024-02-01", 80, false),
        ("C003", "CU003", "Premium", "2023-03-01", "2024-03-01", 150, true),
        ("C004", "CU004", "Basic", "2023-04-01", "2024-04-01", 50, false),
        ("C005", "CU005", "Premium", "2023-05-01", "2024-05-01", 200, true),
        ("C006", "CU006", "Standard", "2023-06-01", "2024-06-01", 90, false),
        ("C007", "CU007", "Premium", "2023-07-01", "2024-07-01", 160, true),
        ("C008", "CU008", "Basic", "2023-08-01", "2024-08-01", 70, false),
        ("C009", "CU009", "Premium", "2023-09-01", "2024-09-01", 110, true),
        ("C010", "CU010", "Standard", "2023-10-01", "2024-10-01", 60, false)
    ).toDF("churn_id","customer_id","plan_type","signup_date","last_activity_date","monthly_spend","churn_flag")

    //customerChurn.show()

    val customerLifeTime = customerChurn.withColumn("customer_lifetime", datediff(col("last_activity_date"), col("signup_date")))
    customerLifeTime.show()

    customerChurn.filter(col("churn_flag")  === true && col("monthly_spend") > 100).show()

    customerLifeTime.groupBy(col("plan_type"), col("customer_lifetime"))
      .agg(sum(col("monthly_spend")).as("total_monthly_spend"), avg(col("customer_lifetime")),
      count(when(col("churn_flag") === true, 1).otherwise(0)).as("count_churn_customers")).show()

    //Use the lead function to estimate potential churn by tracking changes in monthly_spend for
    //customers over time
    val window = Window.partitionBy("customer_id").orderBy("last_activity_date")
    val customerChurnWithDates = customerChurn
      .withColumn("signup_date", to_date(col("signup_date")))
      .withColumn("last_activity_date", to_date(col("last_activity_date")))

    val customerMonthlySpend = customerChurnWithDates.withColumn("next_monthly_spend", lead(col("monthly_spend"), 1).over(window))
    customerMonthlySpend.show()

    customerMonthlySpend.select(col("*"), (col("monthly_spend") - col("next_monthly_spend")).as("diff_monthly_spend")).show()

    println("================== Spark SQL =======================")
    customerChurn.createOrReplaceTempView("customer_churn")
    customerLifeTime.createOrReplaceTempView("customer_lifetime_tab")

    spark.sql(
      """
        SELECT *,
        datediff(last_activity_date, signup_date) as customer_lifetime
        FROM customer_churn
        """).show()

    spark.sql(
      """
        SELECT *
        FROM customer_churn
        WHERE churn_flag = true
        and monthly_spend > 100
        """).show()

    spark.sql(
      """
        SELECT
        plan_type, customer_lifetime,
        sum(monthly_spend) AS total_monthly_spend,
        avg(customer_lifetime),
        count(CASE WHEN churn_flag = true THEN 1 ELSE 0 END) as count_churn_customers
        FROM customer_lifetime_tab
        GROUP BY plan_type, customer_lifetime
        """).show()

    spark.sql(
      """
        SELECT *,
        (monthly_spend -
        LEAD(monthly_spend, 1) OVER(partition by customer_id order by last_activity_date))
        AS diff_monthly_spend
        FROM customer_lifetime_tab
        """).show()

  }
}
