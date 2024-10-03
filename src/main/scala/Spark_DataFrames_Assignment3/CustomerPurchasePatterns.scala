package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lead, min, month, sum}

object CustomerPurchasePatterns {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("CustomerPurchasePatterns")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val customerPurchases = List(
        ("P001", "C001", "Running Shoes", "2023-10-10", 100, "Credit Card"),
        ("P002", "C002", "Basketball Shoes", "2023-11-01", 150, "Debit Card"),
        ("P003", "C003", "Soccer Shoes", "2023-12-01", 120, "Credit Card"),
        ("P004", "C001", "Dress Shoes", "2024-01-15", 200, "Credit Card"),
        ("P005", "C004", "Running Shoes", "2024-02-10", 110, "Cash"),
        ("P006", "C001", "Casual Shoes", "2024-02-15", 90, "Credit Card"),
        ("P007", "C002", "Sneakers", "2024-03-01", 130, "Credit Card"),
        ("P008", "C003", "Formal Shoes", "2024-03-05", 160, "Credit Card"),
        ("P009", "C001", "Sports Shoes", "2024-03-10", 115, "Credit Card"),
        ("P010", "C002", "Running Shoes", "2024-04-01", 140, "Debit Card")
    ).toDF("purchase_id","customer_id","product_name","purchase_date","purchase_amount","payment_method")

    //customerPurchases.show()

    val customerPurchaseMonth = customerPurchases.withColumn("purchase_month", month(col("purchase_date")))

    customerPurchaseMonth.show()

    customerPurchases.filter(col("product_name").endsWith("Shoes")
      && col("payment_method").startsWith("Credit")).show()

    customerPurchaseMonth.groupBy(col("customer_id"), col("purchase_month"))
      .agg(sum(col("purchase_amount")).as("total_purchase_amount"),
      min(col("purchase_amount")), count(col("purchase_id"))).show()

    //Use the lead function to calculate the difference in purchase_amount between consecutive
    //purchases for each customer
    val window = Window.partitionBy("customer_id").orderBy("purchase_date")
    val customerNextPurchaseDF = customerPurchases.select(col("*"),
        lead(col("purchase_amount"), 1).over(window).as("next_purchase_amount"))

    customerNextPurchaseDF.select(col("*"), col("next_purchase_amount") - col("purchase_amount")).show()

    println("================== Spark SQL ====================")
    customerPurchases.createOrReplaceTempView("customer_purchases")
    customerPurchaseMonth.createOrReplaceTempView("customer_purchase_month")

    spark.sql(
      """
        SELECT *,
        EXTRACT(month FROM purchase_date) as purchase_month
        FROM customer_purchases
        """).show()

    spark.sql(
      """
        SELECT *
        FROM customer_purchases
        WHERE product_name like '%Shoes'
        and payment_method like 'Credit%'
        """).show()

    spark.sql(
      """
        SELECT
        customer_id, purchase_month,
        sum(purchase_amount), min(purchase_amount),
        count(purchase_id)
        FROM customer_purchase_month
        GROUP BY customer_id, purchase_month
        """).show()

    spark.sql(
      """
        SELECT *,
        LEAD(purchase_amount, 1) OVER(partition by customer_id ORDER BY purchase_date) -
        purchase_amount
        as diff_purchase_amount
        FROM customer_purchase_month
        """).show()

  }
}
