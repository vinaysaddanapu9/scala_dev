package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object LeadProblem3 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LeadProblem3")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
      val customers1 = List(
         (123, "JOHN",400),
         (145, "JOHN",700),
         (178, "Bob",300),
         (897, "BOB",400)
      ).toDF("ORDER_ID","CUST_NAME","AMOUNT")

    //orders.show()
    customers1.createOrReplaceTempView("customers1")

    //LEAD
    spark.sql(
      """
        SELECT
        ORDER_ID, CUST_NAME,
        AMOUNT, LEAD(AMOUNT, 1) OVER(PARTITION BY CUST_NAME ORDER BY AMOUNT) AS AMNT
        FROM customers1
        """).show()

  }
}
