package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object TransactionAmountsDateAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("TransactionAmountsAndDateAnalysis")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
     val transactions = List(
      (1, "2023-12-01", 1200, "Credit"),
      (2, "2023-11-15", 600, "Debit"),
      (3, "2023-12-20", 300, "Credit"),
      (4, "2023-10-10", 1500, "Debit"),
      (5, "2023-12-30", 250, "Credit"),
      (6, "2023-09-25", 700, "Debit")
     ).toDF("transaction_id", "transaction_date", "amount", "transaction_type")

    //transactions.show()

    transactions.select(col("*"), when(col("amount") > 1000, "High")
      .when((col("amount") >= 500) && (col("amount") <= 1000), "Medium")
      .when(col("amount") < 500, "Low").as("amount_category")).show()

    //Create a new column transaction_month that extracts the month from transaction_date.

    //Filter transactions that occurred in the month of 'December'

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) amount for
    //each transaction_type
    transactions.groupBy(col("transaction_type")).agg(sum(col("amount")).as("total_sum"), avg(col("amount")),
      min(col("amount")), max(col("amount"))).show()

    println("============= Spark SQL ==============")

    transactions.createOrReplaceTempView("transactions")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN amount > 1000 THEN 'High'
        WHEN amount BETWEEN 500 and 1000 THEN 'Medium'
        WHEN amount < 500 THEN 'Low'
        END as amount_category
        FROM transactions
        """).show()

    spark.sql(
      """
        SELECT transaction_type,
        sum(amount) as total_sum, avg(amount),
        max(amount), min(amount)
        FROM transactions
        GROUP BY transaction_type
        """).show()

  }
}
