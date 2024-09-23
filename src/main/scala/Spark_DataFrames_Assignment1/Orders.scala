package Spark_DataFrames_Assignment1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Orders {

  def main(args: Array[String]) : Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("OrdersExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val orders = List(
      (1, 5, 100),
      (2, 10, 150),
      (3, 20, 300)
    ).toDF("order_id", "quantity", "total_price")

    orders.withColumn("order_type", when((col("quantity") < 10) && (col("total_price") < 200), "Small & Cheap")
    .when((col("quantity") >= 10) && (col("total_price") < 200), "Bulk & Discounted")
      .otherwise("Premium Order")).show()

    orders.createOrReplaceTempView("orders")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN quantity < 10 and total_price < 200 THEN 'Small & Cheap'
        WHEN quantity >= 10 and total_price < 200 THEN 'Bulk & Discounted'
        ELSE 'Premium Order'
        END AS order_type
        FROM orders
        """).show()

    //orders.show()
    
  }
}
