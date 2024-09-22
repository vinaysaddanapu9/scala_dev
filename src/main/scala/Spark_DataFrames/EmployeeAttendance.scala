package Spark_DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object EmployeeAttendance {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Employee_Attendance")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = List((1, "HR", 25, "2026-03"),(2, "IT", 18, "2026-03"), (3, "Sales", 7, "2026-03"),
    (4, "IT", 20, "2026-03"),(5, "HR", 22, "2026-03"),(6, "Sales", 12, "2026-03"))
      .toDF("employee_id", "department", "attendance_days", "attendance_month")

    data.withColumn("attendance_category", when(col("attendance_days") >= 22, "Excellent")
    .when((col("attendance_days") > 15) && (col("attendance_days") < 22), "Good")
      .when((col("attendance_days") > 8) && (col("attendance_days") < 15), "Average").otherwise("Poor"))
      .show()

    //data.show()
  }
}
