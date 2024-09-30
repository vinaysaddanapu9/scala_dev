package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lag, lit, month, when}

object EmployeePerformanceAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("EmployeePerformanceAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val  employeePerformance = List(
        ("E001", "Sales", 85, "2024-02-10", "Sales Manager"),
        ("E002", "HR", 78, "2024-03-15", "HR Assistant"),
        ("E003", "IT", 92, "2024-01-22", "IT Manager"),
        ("E004", "Sales", 88, "2024-02-18", "Sales Rep"),
        ("E005", "HR", 95, "2024-03-20", "HR Manager")
    ).toDF("employee_id", "department", "performance_score", "review_date", "position")

    //employeePerformance.show()
    //Create a new column review_month using month extracted from review_date

    val reviewMonthDF = employeePerformance.withColumn("review_month", month(col("review_date")))

    reviewMonthDF.show()

    //Filter records where the position ends with 'Manager' and the performance_score is greater
    //than 80

    employeePerformance.filter(col("position").endsWith("Manager") && col("performance_score") > 80).show()

    reviewMonthDF.groupBy(col("department"), col("review_month")).agg(avg(col("performance_score")),
      count(when(col("performance_score") > 90, 1).otherwise(0)).as("count_employee")
    ).show()

    val window = Window.orderBy("review_date")

    val previousPerformanceDF = employeePerformance.select(col("*"),
      lag(col("performance_score"), 1).over(window).as("previous_performance"))

    previousPerformanceDF.withColumn("performance_status",
      when(col("performance_score") > col("previous_performance"), lit("Improvement"))
        .when(col("previous_performance").isNull, lit("NA"))
        .otherwise(lit("Decline"))).show()

    println("======================= Spark SQL =======================")

    employeePerformance.createOrReplaceTempView("employee_performance")
    reviewMonthDF.createOrReplaceTempView("review_month_tab")

      spark.sql(
        """
          SELECT *,
          EXTRACT(month FROM review_date) AS review_month
          FROM employee_performance
          """).show()

    spark.sql(
      """
        SELECT *
        FROM employee_performance
        WHERE position like '%Manager'
        and performance_score > 80
        """).show()

    spark.sql(
      """
        SELECT
        department, review_month,
        avg(performance_score),
        count(CASE WHEN performance_score > 90 THEN 1 ELSE 0 END) as count_employees
        FROM review_month_tab
        GROUP BY department, review_month
        """).show()

    val previousPerformance1 = spark.sql(
      """
        SELECT *,
        lag(performance_score) over(order by review_date) as previous_performance
        FROM employee_performance
        """)

    previousPerformance1.createOrReplaceTempView("previous_performance_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN performance_score > previous_performance THEN 'Improvement'
        WHEN previous_performance IS NULL THEN 'NA' ELSE 'Decline'
        END AS performance_status
        FROM previous_performance_tab
        """).show()

  }
}
