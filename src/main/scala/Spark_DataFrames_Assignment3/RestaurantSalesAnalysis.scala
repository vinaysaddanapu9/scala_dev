package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lead, month, sum, when}

object RestaurantSalesAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("RestaurantSalesAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val restaurantSalesData = List(
        ("S001", "R001", "Pepperoni Pizza", "2023-10-01", 20, "Credit Card"),
        ("S002", "R002", "Veggie Pizza", "2023-11-05", 15, "Cash"),
        ("S003", "R001", "Cheese Pizza", "2023-12-10", 18, "Credit Card"),
        ("S004", "R003", "BBQ Chicken Pizza", "2023-12-15", 22, "Debit Card"),
        ("S005", "R002", "Margherita Pizza", "2024-01-01", 16, "Credit Card"),
        ("S006", "R001", "Mushroom Pizza", "2024-01-10", 25, "Credit Card"),
        ("S007", "R003", "Spicy Pizza", "2024-01-15", 30, "Credit Card"),
        ("S008", "R002", "Hawaiian Pizza", "2024-02-05", 28, "Cash")
    ).toDF("sale_id","restaurant_id","item_name","sale_date","sale_amount","payment_type")

    //restaurantSalesData.show()

    val restaurantSalesMonthDF = restaurantSalesData.withColumn("sale_month", month(col("sale_date")))

    restaurantSalesMonthDF.show()

    restaurantSalesData.filter(col("item_name").startsWith("Pizza")
      && col("payment_type") === "Credit Card").show()

    restaurantSalesMonthDF.groupBy(col("sale_month"), col("restaurant_id"))
      .agg(sum(col("sale_amount")).as("total_sale_amount"), avg(col("sale_amount"))
      ,count(when(col("payment_type") === "Credit Card", 1)).as("count_credit_card_trans")).show()

    //Use the lead function to estimate the next month’s sales for each restaurant
    val window = Window.partitionBy("restaurant_id").orderBy("sale_date")
    restaurantSalesData.select(col("*"),
      lead(col("sale_amount"), 1).over(window).as("next_month_sales"))
      .orderBy(col("restaurant_id"), col("sale_date")).show()

    println("============== Spark SQL ===============")
    restaurantSalesData.createOrReplaceTempView("restaurant_sales")
    restaurantSalesMonthDF.createOrReplaceTempView("restaurant_sales_month_tab")

    spark.sql(
      """
        SELECT *,
        EXTRACT(month FROM sale_date) AS sale_month
        FROM restaurant_sales
        """).show()

    spark.sql(
      """
        SELECT *
        FROM restaurant_sales
        WHERE item_name like 'Pizza%' and payment_type = 'Credit Card'
        """).show()

    spark.sql(
      """
        SELECT
        restaurant_id, sale_date,
        sum(sale_amount) as total_sale_amount,
        avg(sale_amount),
        count(CASE WHEN payment_type = 'Credit Card' THEN 1 END) as count_credit_card_trans
        FROM restaurant_sales_month_tab
        GROUP BY restaurant_id, sale_date
        """).show()

    spark.sql(
      """
        SELECT *,
        LEAD(sale_amount, 1) OVER(partition by restaurant_id order by sale_date) as next_month_sales
        FROM restaurant_sales
        ORDER BY restaurant_id, sale_date
        """).show()

  }
}
