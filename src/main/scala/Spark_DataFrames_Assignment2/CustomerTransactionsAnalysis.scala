package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CustomerTransactionsAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("CustomerTransactionsAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val transactions = List(
      (1, 1, 1200, "2024-01-15"),
      (2, 2, 600, "2024-01-20"),
      (3, 3, 300, "2024-02-15"),
      (4, 4, 1500, "2024-02-20"),
      (5, 5, 200, "2024-03-05"),
      (6, 6, 900, "2024-03-12")
    ).toDF("transaction_id", "customer_id", "transaction_amount", "transaction_date")

    transactions.show()





  }
}
