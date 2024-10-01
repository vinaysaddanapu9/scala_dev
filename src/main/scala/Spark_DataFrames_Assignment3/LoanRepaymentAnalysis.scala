package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, dayofmonth, lead, max, sum, when}

object LoanRepaymentAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LoanRepaymentAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
      val loanRepayment = List(
          ("L001", "C001", 1000, "2023-11-01", "2023-12-01", "Personal Loan", "7.5"),
          ("L002", "C002", 2000, "2023-11-15", "2023-11-20", "Home Loan", "6.5"),
          ("L003", "C003", 1500, "2023-12-01", "2023-12-25", "Personal Loan", "8.0"),
          ("L004", "C004", 2500, "2023-12-10", "2024-01-15", "Car Loan", "9.0"),
          ("L005", "C005", 1200, "2023-12-15", "2024-01-20", "Personal Loan", "7.8")
      ).toDF("loan_id","customer_id","repayment_amount","due_date","payment_date","loan_type","interest_rate")

    //loanRepayment.show()
    //Create a new column repayment_delay by calculating the difference in days between
    //payment_date and due_date
    val daysDF = loanRepayment.withColumn("day_due_date", dayofmonth(col("due_date")))
      .withColumn("day_payment_date", dayofmonth(col("payment_date")))

    val repaymentDelayDF = daysDF.withColumn("repayment_delay",
      col("day_payment_date") - col("day_due_date"))

    repaymentDelayDF.show()

    repaymentDelayDF.filter(col("loan_type").startsWith("Personal") && col("repayment_delay") > 30).show()

    repaymentDelayDF.groupBy(col("loan_type"), col("interest_rate")).agg(
      sum(col("repayment_amount")), max(col("repayment_delay")),
        avg(when(col("repayment_delay") > 0, col("interest_rate"))).as("avg_interest_rate"))
      .show()

    val window = Window.orderBy("due_date")
    loanRepayment.select(col("*"), lead(col("repayment_amount"), 1).over(window).as("next_repayment_amount"))
      .show()

  }
}
