package Spark_DataFrames_Assignment1

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{col, when}

object StringManipulationCustomers {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("CustomersExample")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._

    val customers = List(
      (1, "john@gmail.com"),
      (2, "jane@yahoo.com"),
      (3, "doe@hotmail.com")
    ).toDF("customer_id", "email")

    customers.withColumn("email_provider",
      when(col("email").like("%gmail%") , "Gmail").when(col("email").like("%yahoo%"), "Yahoo")
        .otherwise("Others")).show()

    customers.createOrReplaceTempView("customers")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN email like '%gmail%' THEN 'Gmail'
        WHEN email like '%yahoo%' THEN 'Yahoo'
        ELSE 'Other'
        END AS email_provider
        FROM customers
        """).show()

    //customers.show()

  }
}
