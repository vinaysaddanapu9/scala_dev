package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object EventAttendanceTracking {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

   val spark = SparkSession.builder()
               .appName("EventAttendanceTracking")
               .master("local[*]")
               .getOrCreate()

    import spark.implicits._
    val eventAttendance = List(
        (1, "Tech Conference", 600, "2024-01-10"),
        (2, "Sports Event", 250, "2024-01-15"),
        (3, "Tech Expo", 700, "2024-01-20"),
        (4, "Music Festival", 150, "2024-02-01"),
        (5, "Tech Seminar", 300, "2024-02-10"),
        (6, "Art Exhibition", 400, "2024-03-01")
    ).toDF("event_id", "event_name", "attendees", "event_date")

    //eventAttendance.show()
    val eventAttendanceStatusDF = eventAttendance.withColumn("attendance_status", when(col("attendees") > 500, "Full")
      .when((col("attendees") >= 200) and (col("attendees") <= 500), "Moderate")
      .when(col("attendees") < 200, "Low"))

    eventAttendanceStatusDF.show()

    eventAttendance.filter(col("event_name").startsWith("Tech")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) attendees for
    //each attendance_status
    eventAttendanceStatusDF.groupBy(col("attendance_status")).agg(sum(col("attendees")).as("total_sum_attendees"), avg(col("attendees")),
      min(col("attendees")), max(col("attendees"))).show()

    println("================= Spark SQL ======================")
    eventAttendance.createOrReplaceTempView("event_attendance")
    eventAttendanceStatusDF.createOrReplaceTempView("event_attendance_status_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN attendees > 500 THEN 'Full'
        WHEN attendees BETWEEN 200 and 500 THEN 'Moderate'
        WHEN attendees < 200 THEN 'Low'
        END AS attendance_status
        FROM event_attendance
        """).show()

     spark.sql(
       """
         SELECT *
         FROM event_attendance
         WHERE event_name like 'Tech%'
         """).show()

    spark.sql(
      """
        SELECT
        attendance_status,
        sum(attendees) as total_sum_attendees, avg(attendees),
        min(attendees), max(attendees)
        FROM event_attendance_status_tab
        GROUP BY attendance_status
        """).show()

  }
}
