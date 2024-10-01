package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lag, sum, year}

object ProductReturnAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("ProductReturnAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
     val productReturns = List(
        ("R001", "O001", "Electro Gadgets", "2023-12-01", "Damaged", 100),
        ("R002", "O002", "Home Appliances", "2023-12-05", "Defective", 50),
        ("R003", "O003", "Electro Toys", "2023-12-10", "Changed Mind", 75),
        ("R004", "O004", "Electro Gadgets", "2023-12-15", "Damaged", 100),
        ("R005", "O005", "Kitchen Set", "2023-12-20", "Wrong Product", 120)
     ).toDF("return_id","order_id","product_name","return_date","return_reason","refund_amount")

    //productReturns.show()
    //Create a new column return_year using year extracted from return_date
    val returnYearDF = productReturns.withColumn("return_year", year(col("return_date")))

    returnYearDF.show()

    productReturns.filter(col("product_name").startsWith("Electro") && col("refund_amount").isNotNull).show()

    returnYearDF.groupBy(col("return_year"), col("return_reason")).agg(sum(col("refund_amount")).as("total_refund_amount"),
        count(col("return_id")), avg(col("refund_amount"))).show()

    //Use the lag function to compare each product's refund amount with its previous return
    val window = Window.partitionBy("product_name").orderBy("return_date")
    val prevRefundAmountDF = productReturns.select(col("*"),
      lag(col("refund_amount"),1).over(window).as("prev_refund_amount"))

    prevRefundAmountDF.select(col("*"), (col("refund_amount") - col("prev_refund_amount"))
      .as("diff_refund_amount")).show()

    println("=============== Spark SQL ===============")
    productReturns.createOrReplaceTempView("product_returns")
    returnYearDF.createOrReplaceTempView("return_year_tab")

    spark.sql(
      """
        SELECT *,
        EXTRACT(year FROM return_date) as return_year
        FROM product_returns
        """).show()

    spark.sql(
      """
        SELECT *
        FROM product_returns
        WHERE
        product_name like 'Electro%' and refund_amount IS NOT NULL
        """).show()

    spark.sql(
      """
        SELECT
        return_year, return_reason,
        sum(refund_amount) as total_refund_amount,
        count(return_id), avg(refund_amount)
        FROM return_year_tab
        GROUP BY return_year, return_reason
        """).show()

    spark.sql(
      """
        SELECT *,
        refund_amount -
        LAG(refund_amount, 1) OVER(partition by product_name ORDER BY return_date)
        AS diff_refund_amount
        FROM product_returns
        """).show()

  }
}
