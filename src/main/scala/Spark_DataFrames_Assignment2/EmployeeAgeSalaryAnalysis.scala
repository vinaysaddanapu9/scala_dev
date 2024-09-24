package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object EmployeeAgeSalaryAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("EmployeeAgeAndSalaryAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val employees = List(
          (1, "John", 28, 60000),
          (2, "Jane", 32, 75000),
          (3, "Mike", 45, 120000),
          (4, "Alice", 55, 90000),
          (5, "Steve", 62, 110000),
          (6, "Claire", 40, 40000)
    ).toDF("employee_id", "name", "age", "salary")

    //Age Group
    employees.select(col("*"), when(col("age") < 30, "Young")
      .when((col("age") >= 30) && (col("age") <= 50), "Mid").when(col("age") > 50, "Senior").as("age_group"))
      .show()

    //Salary Range
    employees.select(col("*"), when(col("salary") > 100000, "High")
      .when((col("salary") >= 50000) && (col("salary") <= 100000), "Medium")
       .when(col("salary") < 50000, "Low").as("salary_range")).show()

    //Filter
    employees.filter(col("name").startsWith("J")).show()
    employees.filter(col("name").endsWith("e")).show()

    //employees.show()
    println("=============== Spark SQL ===================")

    employees.createOrReplaceTempView("employees")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN age < 30 THEN 'Young'
        WHEN age BETWEEN 30 and 50 THEN 'Mid'
        WHEN age > 50 THEN 'Senior'
        END AS age_group
        FROM employees
        """).show()

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN salary > 100000 THEN 'High'
        WHEN salary BETWEEN 50000 and 100000 THEN 'Medium'
        WHEN salary < 50000 THEN 'Low'
        END AS salary_range
        FROM employees
        """).show()

    spark.sql(
      """
        SELECT *
        FROM employees
        WHERE name like 'J%'
        """).show()

    spark.sql(
      """
        SELECT *
        FROM employees
        WHERE name like '%e'
        """).show()

  }
}
