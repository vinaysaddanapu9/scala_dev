package Spark_DataFrames_Assignment1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Email {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("EmailDomainExample")
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._

    val emails = List(
      (1, "user@gmail.com"),
      (2, "admin@yahoo.com"),
      (3, "info@hotmail.com")
    ).toDF("email_id", "email_address")

    emails.withColumn("email_domain", when(col("email_address").like("%gmail%"), "Gmail")
    .when(col("email_address").like("%yahoo%"), "Yahoo").otherwise("Hotmail")).show()

    emails.createOrReplaceTempView("emails")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN email_address like '%gmail%' THEN 'Gmail'
        WHEN email_address like '%yahoo%' THEN 'Yahoo'
        ELSE 'Hotmail'
        END AS email_domain
        FROM emails
        """).show()

    //emails.show()

  }
}
