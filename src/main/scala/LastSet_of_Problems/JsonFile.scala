package LastSet_of_Problems

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{column, month, sum}

object JsonFile {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("ReadJSONFile")
      .master("local[*]")
      .getOrCreate()

    val customersDF = spark.read.format("json")
                      .option("multiline", true)
                      .option("header", true)
                      .option("inferSchema", true)
                      .option("path", "E://files/customers.json")
                      .load()

    customersDF.show()
    val transactionsDF = spark.read.format("json")
                      .option("multiline", true)
                      .option("header", true)
                      .option("inferSchema", true)
                      .option("path","E://files/transactions.json")
                      .load()

    transactionsDF.show()

    val condition = customersDF("customer_id") === transactionsDF("customer_id")
    val joinType = "inner"

    val joinedDF = customersDF.join(transactionsDF, condition, joinType)
    val extractedDF = joinedDF.withColumn("month", month(column("transaction_date"))).drop(transactionsDF("customer_id"))

    val filled_df = extractedDF.na.fill(Map("amount" -> 000.00))
    filled_df.show()

    val aggregatedDF = filled_df.groupBy(column("month")).agg(sum(column("amount")).alias("total_transaction"))

    aggregatedDF.write.format("parquet")
      .option("header", true)
      .option("path", "E://files/json_output")
      .save()


  }

}
