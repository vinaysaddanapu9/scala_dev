package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object CustomerSupportTickets {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("CustomerSupportTickets")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val supportTickets = List(
        (1, "Bug", 1.5, "High"),
        (2, "Feature", 3.0, "Medium"),
        (3, "Bug", 4.5, "Low"),
        (4, "Bug", 2.0, "High"),
        (5, "Enhancement", 1.0, "Medium"),
        (6, "Bug",5.0, "Low")
    ).toDF("ticket_id", "issue_type", "resolution_time", "priority")

    //supportTickets.show()
    val ticketsResolutionStatusDF = supportTickets.withColumn("resolution_status",
      when(col("resolution_time") <= 2.0, "Quick")
      .when((col("resolution_time") > 2.0) and (col("resolution_time") <= 4.0), "Moderate")
      .when(col("resolution_time") > 4.0, "Slow"))

    ticketsResolutionStatusDF.show()

    supportTickets.filter(col("issue_type").contains("Bug")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //resolution_time for each resolution_status
    ticketsResolutionStatusDF.groupBy(col("resolution_status")).agg(sum(col("resolution_time")).as("total_resolution_time"),
      avg(col("resolution_time")), min(col("resolution_time")), max(col("resolution_time"))).show()

    println("=============== Spark SQL ======================")
    supportTickets.createOrReplaceTempView("support_tickets")
    ticketsResolutionStatusDF.createOrReplaceTempView("tickets_resolution_status_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN resolution_time <= 2.0 THEN 'Quick'
        WHEN resolution_time BETWEEN 2.0 and 4.0 THEN 'Moderate'
        WHEN resolution_time > 4.0 THEN 'Slow'
        END AS resolution_status
        FROM support_tickets
        """).show()

    spark.sql(
      """
        SELECT *
        FROM support_tickets
        WHERE issue_type like '%Bug%'
        """).show()

    spark.sql(
      """
        SELECT
        resolution_status,
        sum(resolution_time) as total_resolution_time, avg(resolution_time),
        min(resolution_time), max(resolution_time)
        FROM tickets_resolution_status_tab
        GROUP BY resolution_status
        """).show()

  }
}
