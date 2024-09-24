package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

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
    val df1 = employees.select(col("*"), when(col("age") < 30, "Young")
      .when((col("age") >= 30) && (col("age") <= 50), "Mid").when(col("age") > 50, "Senior").as("age_group"))

    df1.show()

    //Salary Range
    employees.select(col("*"), when(col("salary") > 100000, "High")
      .when((col("salary") >= 50000) && (col("salary") <= 100000), "Medium")
       .when(col("salary") < 50000, "Low").as("salary_range")).show()

    //Filter
    employees.filter(col("name").startsWith("J")).show()
    employees.filter(col("name").endsWith("e")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) salary for each age_group.
    df1.groupBy(col("age_group")).agg(sum(col("salary")).as("total_salary"), avg(col("salary")), min(col("salary")),
      max(col("salary"))).show()

    //employees.show()
    println("=============== Spark SQL ===================")

    employees.createOrReplaceTempView("employees")
    df1.createOrReplaceTempView("emp_age_group")

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

    spark.sql(
      """
        SELECT
        age_group,
        sum(salary) as total_salary, avg(salary),
        max(salary), min(salary)
        FROM emp_age_group
        GROUP BY age_group
        """).show()

  }
}
