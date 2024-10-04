package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dayofmonth, lead, max, min, sum}

object DailyWebsiteTrafficAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("DailyWebsiteTrafficAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val webTraffic = List(
      ("S001", "U001", "https://www.shop.com", "2024-02-01", "12:30:00", "Chrome", 15),
      ("S002", "U002", "https://www.news.com", "2024-02-01", "13:00:00", "Firefox", 30),
      ("S003", "U003", "https://www.blog.com", "2024-02-01", "14:00:00", "Safari", 25),
      ("S004", "U001", "https://www.shop.com", "2024-02-02", "12:45:00", "Chrome", 20),
      ("S005", "U004", "https://www.news.com", "2024-02-02", "15:00:00", "Firefox", 35),
      ("S006", "U001", "https://www.shop.com", "2024-02-02", "16:00:00", "Chrome", 10),
      ("S007", "U002", "https://www.tech.com", "2024-02-03", "17:30:00", "Chrome", 22),
      ("S008", "U001", "https://www.shop.com", "2024-02-03", "11:00:00", "Firefox", 35),
      ("S009", "U005", "https://www.blog.com", "2024-02-03", "14:15:00", "Safari", 40),
      ("S010", "U006", "https://www.shop.com", "2024-02-04", "09:30:00", "Chrome", 55)
    ).toDF("session_id","user_id","page_url","visit_date","visit_time","browser","session_duration")

    //webTraffic.show()

    val visitDayDF = webTraffic.withColumn("visit_day", dayofmonth(col("visit_date")))
    visitDayDF.show()

    webTraffic.filter(col("page_url").startsWith("https://www") && col("browser").isNotNull).show()

    visitDayDF.groupBy(col("browser"), col("visit_day"))
      .agg(sum(col("session_duration")), min(col("session_duration")), max(col("session_duration"))).show()

    visitDayDF.groupBy(col("page_url")).agg(count(col("page_url"))).show()

    //Use the lead function to identify any drop in session_duration for each user from one visit to the next
     val window = Window.partitionBy("user_id").orderBy("visit_date")
     val webTrafficSessionDF = webTraffic.select(col("*"),
       lead(col("session_duration"), 1).over(window).as("next_session_duration"))

    webTrafficSessionDF.select(col("*"),
      (col("session_duration") - col("next_session_duration")).as("diff_session_duration")).show()

    println("================ Spark SQL ===================")
    webTraffic.createOrReplaceTempView("web_traffic")
    visitDayDF.createOrReplaceTempView("visit_day_tab")

    spark.sql(
      """
        SELECT *,
        EXTRACT(day FROM visit_date) AS visit_day
        FROM web_traffic
        """).show()

    spark.sql(
      """
        SELECT *
        FROM web_traffic
        WHERE page_url like 'https://www%'
        and browser is not null
        """).show()

    spark.sql(
      """
        SELECT
        browser, visit_day,
        sum(session_duration), min(session_duration),
        max(session_duration)
        FROM visit_day_tab
        GROUP BY browser, visit_day
        """).show()

    spark.sql(
      """
        SELECT
        page_url, count(page_url)
        FROM visit_day_tab
        GROUP BY page_url
        """).show()

    spark.sql(
      """
        SELECT *,
        session_duration - LEAD(session_duration, 1) OVER(partition by user_id order by visit_date)
        as diff_session_duration
        FROM web_traffic
        """).show()
  }
}
