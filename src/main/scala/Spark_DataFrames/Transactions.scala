package Spark_DataFrames

import org.apache.spark.sql.SparkSession

object Transactions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                .appName("Spark_Transactions")
                .master("local[*]").getOrCreate()

    import spark.implicits._

    val transactions = List(
      (1, 1000),
      (2, 200),
      (3, 5000)
    ).toDF("transaction_id", "amount")

    transactions.show()


  }
}
