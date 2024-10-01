package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lag, lead, month, sum}

object WebsiteTrafficAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("WebsiteTrafficAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val websiteTraffic = List(
      ("S001", "U001", 15, "2023-10-01", "Mobile", "Organic"),
      ("S002", "U002", 10, "2023-10-05", "Desktop", "Paid"),
      ("S003", "U003", 20, "2023-10-10", "Mobile", "Organic"),
      ("S004", "U004", 25, "2023-10-15", "Tablet", "Referral"),
      ("S005", "U001", 30, "2023-11-01", "Mobile", "Organic")
    ).toDF("session_id","user_id","page_views","session_date","device_type","traffic_source")

    //websiteTraffic.show()
    //Create a new column session_month using month extracted from session_date.
    val sessionMonthDF = websiteTraffic.withColumn("session_month", month(col("session_date")))

    sessionMonthDF.show()

   websiteTraffic.filter(col("traffic_source") === "Organic" && col("device_type") === "Mobile").show()

    sessionMonthDF.groupBy(col("device_type"), col("session_month")).agg(sum(col("page_views")),
      avg(col("page_views")), count(col("session_id"))).show()

    val window = Window.partitionBy(col("user_id")).orderBy(col("session_date"))
    val pageviewsDF = websiteTraffic.select(col("*"),lead(col("page_views"), 1).over(window).as("next_page_views"))

    pageviewsDF.select(col("*"), (col("next_page_views") - col("page_views")).as("diff_page_views")).show()

    println("=================== Spark SQL ========================")
    websiteTraffic.createOrReplaceTempView("website_traffic")
    sessionMonthDF.createOrReplaceTempView("session_month_tab")

    spark.sql(
      """
        SELECT *,
        EXTRACT(month FROM session_date) as session_month
        FROM website_traffic
        """).show()

    spark.sql(
      """
        SELECT *
        FROM website_traffic
        WHERE traffic_source = 'Organic' and device_type = 'Mobile'
        """).show()

    spark.sql(
      """
        SELECT
        device_type, session_month,
        sum(page_views), avg(page_views),
        count(session_id)
        FROM session_month_tab
        GROUP BY device_type, session_month
        """).show()

    spark.sql(
      """
        SELECT *,
        (LEAD(page_views,1) over(partition by user_id order by session_date) - page_views) as diff_page_views
        FROM website_traffic
        """).show()

  }
}
