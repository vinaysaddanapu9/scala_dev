package Spark_DataFrames_Assignment1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

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

    transactions.withColumn("category", when(col("amount") > 1000, "High")
      .when((col("amount") > 500) && (col("amount") <= 1000), "Medium").otherwise("Low")).show()

    //transactions.show()


  }
}
