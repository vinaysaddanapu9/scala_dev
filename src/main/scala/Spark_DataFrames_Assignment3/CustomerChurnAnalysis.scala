package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lead, year}
import org.apache.spark.sql.expressions.Window

object CustomerChurnAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("CustomerChurnAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
     val customerChurn = List(
        ("C001", "Premium Gold", "Yes", "2023-12-01", 1200, "USA"),
        ("C002", "Basic", "No", null, 400, "Canada"),
        ("C003", "Premium Silver", "Yes", "2023-11-15", 800, "UK"),
        ("C004", "Premium Gold", "Yes", "2024-01-10", 1500, "USA"),
        ("C005", "Basic", "No", null, 300, "India")
    ).toDF("customer_id","subscription_type","churn_status","churn_date","revenue","country")

    //customerChurn.show()
    //Create a new column churn_year using year extracted from churn_date.
     val churnYearDF = customerChurn.withColumn("churn_year", year(col("churn_date")))
     churnYearDF.show()

    customerChurn.filter(col("subscription_type").startsWith("Premium")
      && col("churn_status").isNotNull).show()

    println("==================Spark SQL ===================")
    customerChurn.createOrReplaceTempView("customer_churn")

    spark.sql(
      """
        SELECT *,
        EXTRACT(year FROM churn_date) as churn_year
        FROM customer_churn
        """).show()

    spark.sql(
      """
        SELECT *
        FROM customer_churn
        WHERE subscription_type like 'Premium%'
        and churn_status IS NOT NULL
        """).show()

  }
}
