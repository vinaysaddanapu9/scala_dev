package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object EmployeeWorkHoursAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("EmployeeWorkHoursAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
      val workHours = List(
      (1, "2024-01-10", 9, "Sales"),
      (2, "2024-01-11", 7, "Support"),
      (3, "2024-01-12", 8, "Sales"),
      (4, "2024-01-13", 10, "Marketing"),
      (5, "2024-01-14", 5, "Sales"),
      (6, "2024-01-15", 6, "Support")
      ).toDF("employee_id", "work_date", "hours_worked", "department")

    //workHours.show()
    workHours.select(col("*"), when(col("hours_worked") > 8, "Overtime")
      .when(col("hours_worked") <= 8, "Regular").as("hours_category")).show()

    //Filter Work Hours
    workHours.filter(col("department").startsWith("S")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) hours_worked
    //for each department
    workHours.groupBy(col("department")).agg(sum(col("hours_worked")).as("total_hours_worked"),
      avg(col("hours_worked")), min(col("hours_worked")), max(col("hours_worked"))).show()

    println("========= Spark SQL ============")
    workHours.createOrReplaceTempView("work_hours")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN hours_worked > 8 THEN 'Overtime'
        WHEN hours_worked <= 8 THEN 'Regular'
        END AS hours_category
        FROM work_hours
        """).show()

    spark.sql(
      """
        SELECT *
        FROM work_hours
        WHERE department like 'S%'
        """).show()

    spark.sql(
      """
        SELECT department,
        sum(hours_worked) AS total_hours_worked, avg(hours_worked),
        min(hours_worked), max(hours_worked)
        FROM work_hours
        GROUP BY department
        """).show()
  }

}
