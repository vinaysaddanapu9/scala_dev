package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object EmployeeLeaveRecords {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("EmployeeLeaveRecords")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val leaveRecords = List(
        (1, "Sick", 12, "2024-01-10"),
        (2, "Sick", 7, "2024-01-15"),
        (3, "Sick", 3, "2024-02-20"),
        (4, "Sick", 6, "2024-02-25"),
        (5, "Sick", 2, "2024-03-05"),
        (6, "Casual", 5, "2024-03-12")
    ).toDF("employee_id","leave_type","leave_duration_days", "leave_date")

    //leaveRecords.show()

    val leaveCategoryDF = leaveRecords.withColumn("leave_category", when(col("leave_duration_days") > 10, "Extended")
      .when((col("leave_duration_days") >= 5) and (col("leave_duration_days") <= 10), "Short")
      .when(col("leave_duration_days") < 5, "Casual"))

    leaveCategoryDF.show()

    leaveRecords.filter(col("leave_type").startsWith("Sick")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //leave_duration_days for each leave_category
    leaveCategoryDF.groupBy(col("leave_category")).agg(sum(col("leave_duration_days")), avg(col("leave_duration_days")),
      min(col("leave_duration_days")), max(col("leave_duration_days"))).show()

    println("================ Spark SQL ======================")
    leaveRecords.createOrReplaceTempView("leave_records")
    leaveCategoryDF.createOrReplaceTempView("leave_category_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN leave_duration_days > 10 THEN 'Extended'
        WHEN leave_duration_days BETWEEN 5 and 10 THEN 'Short'
        WHEN leave_duration_days < 5 THEN 'Casual'
        END AS leave_category
        FROM leave_records
        """).show()

    spark.sql(
      """
        SELECT *
        FROM leave_records
        WHERE leave_type like 'Sick%'
        """).show()

    spark.sql(
      """
        SELECT
        leave_category,
        sum(leave_duration_days), avg(leave_duration_days),
        min(leave_duration_days), max(leave_duration_days)
        FROM leave_category_tab
        GROUP BY leave_category
        """).show()

  }
}
