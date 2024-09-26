package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object EmployeeAttendanceTracking {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

   val spark = SparkSession.builder()
               .appName("EmployeeAttendanceTracking")
               .master("local[*]")
               .getOrCreate()

    import spark.implicits._
    val attendance = List(
        (1, "2024-01-10", 9, "Sick"),
        (2, "2024-01-11", 7, "Scheduled"),
        (3, "2024-01-12", 8, "Sick"),
        (4, "2024-01-13", 4, "Scheduled"),
        (5, "2024-01-14", 6, "Sick"),
        (6, "2024-01-15", 8, "Scheduled")
    ).toDF("employee_id", "attendance_date", "hours_worked", "attendance_type")

    //attendance.show()

    val attendanceStatusDf = attendance.withColumn("attendance_status", when(col("hours_worked") >= 8, "Full Day")
      .when(col("hours_worked") < 8, "Half Day"))

    attendanceStatusDf.show()

    attendance.filter(col("attendance_type").startsWith("S")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) hours_worked
    //for each attendance_status

    attendanceStatusDf.groupBy(col("attendance_status")).agg(sum(col("hours_worked")).as("total_hours_worked"), avg(col("hours_worked"))
    , max(col("hours_worked")), min(col("hours_worked"))).show()

    println("=========== Spark SQL ===============")
    attendance.createOrReplaceTempView("attendance")
    attendanceStatusDf.createOrReplaceTempView("attendance_status_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN hours_worked >= 8 THEN 'Full Day'
        WHEN hours_worked < 8 THEN 'Half Day'
        END AS attendance_status
        FROM attendance
        """).show()

    spark.sql(
      """
        SELECT *
        FROM attendance
        WHERE attendance_type like 'S%'
        """).show()

    spark.sql(
      """
        SELECT
        attendance_status,
        sum(hours_worked) as total_hours_worked, avg(hours_worked),
        min(hours_worked), max(hours_worked)
        FROM attendance_status_tab
        GROUP BY attendance_status
        """).show()

  }
}
