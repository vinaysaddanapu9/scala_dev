package Spark_DataFrames_Assignment1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Employees {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

   val spark = SparkSession.builder()
              .appName("EmployeesExample")
              .master("local[*]")
              .getOrCreate()

    import spark.implicits._

    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")

    employees.withColumn("category", when((col("salary") <  35000) && (col("age") < 30), "Young & Low Salary")
      .when((col("salary") > 35000 && col("salary") <= 45000) && (col("age") >  30 && col("age") <= 40), "Middle Aged & Medium Salary")
      .otherwise("Old & High Salary")).show(truncate=false)

    employees.createOrReplaceTempView("employees")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN salary < 35000 and age < 30 THEN 'Young & Low Salary'
        WHEN salary BETWEEN 35000 and 45000 AND age BETWEEN 30 and 40 THEN 'Middle Aged & Medium Salary'
        ELSE 'Old & High Salary'
        END AS category
        FROM employees
        """).show(truncate = false)

    //employees.show()

  }
}
