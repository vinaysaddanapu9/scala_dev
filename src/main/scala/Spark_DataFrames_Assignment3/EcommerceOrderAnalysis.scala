package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, datediff, lag, sum}

object EcommerceOrderAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("OnlineCourseCompletionAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ecommerceOrders = List(
      ("O001", "C001", "Home Electronics", "2024-01-01", "2024-01-05", 500, "Completed"),
      ("O002", "C002", "Kitchen Electronics", "2024-01-02", "2024-01-06", 200, "Completed"),
      ("O003", "C001", "Home Electronics", "2024-01-03", "2024-01-07", 600, "Pending"),
      ("O004", "C003", "Personal Electronics", "2024-01-04", "2024-01-08", 150, "Completed"),
      ("O005", "C002", "Kitchen Electronics", "2024-01-05", "2024-01-09", 300, "Completed"),
      ("O006", "C001", "Home Electronics", "2024-01-06", "2024-01-10", 700, "Completed"),
      ("O007", "C002", "Kitchen Electronics", "2024-01-07", "2024-01-11", 400, "Completed"),
      ("O008", "C003", "Personal Electronics", "2024-01-08", "2024-01-12", 250, "Completed"),
      ("O009", "C001", "Home Electronics", "2024-01-09", "2024-01-13", 550, "Completed"),
      ("O010", "C004", "Home Electronics", "2024-01-10", "2024-01-14", 350, "Pending")
    ).toDF("order_id","customer_id","product_category","order_date","delivery_date","order_value","payment_status")

    //ecommerceOrders.show()

    val ecommOrdersDeliveryTimeDF = ecommerceOrders.withColumn("delivery_time", datediff(col("delivery_date"), col("order_date")))
    ecommOrdersDeliveryTimeDF.show()

    ecommerceOrders.filter(col("product_category").endsWith("Electronics")
      && col("payment_status") === "Completed").show()

    ecommOrdersDeliveryTimeDF.groupBy(col("customer_id"), col("product_category"))
      .agg(sum(col("order_value")), count(col("order_id")), avg(col("delivery_time"))).show()

    //Use the lag function to identify if the delivery_time has increased or decreased for
    //consecutive orders for the same customer
    val window = Window.partitionBy("customer_id").orderBy("delivery_date")
    val deliveryTimeDF = ecommOrdersDeliveryTimeDF.select(col("*"),
      lag(col("delivery_time"), 1).over(window).as("prev_delivery_time"))

    deliveryTimeDF.select(col("*"),
      (col("delivery_time") - col("prev_delivery_time")).as("diff_delivery_time")).show()

    println("============== Spark SQL ==================")
    ecommerceOrders.createOrReplaceTempView("ecommerce_orders")
    ecommOrdersDeliveryTimeDF.createOrReplaceTempView("ecomm_orders_delivery_time_tab")

    spark.sql(
      """
        SELECT *,
        datediff(delivery_date, order_date) AS delivery_time
        FROM ecommerce_orders
        """).show()

    spark.sql(
      """
        SELECT *
        FROM ecommerce_orders
        WHERE product_category like '%Electronics'
        and payment_status = 'Completed'
        """).show()

    spark.sql(
      """
        SELECT
        customer_id, product_category,
        sum(order_value), count(order_id), avg(delivery_time)
        FROM ecomm_orders_delivery_time_tab
        GROUP BY customer_id, product_category
        """).show()

    spark.sql(
      """
        SELECT *,
        delivery_time -
        LAG(delivery_time) OVER(partition by customer_id order by delivery_date)
        AS diff_delivery_time
        FROM ecomm_orders_delivery_time_tab
        """).show()

  }
}
