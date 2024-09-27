package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object EmployeeSalaryDistribution {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("EmployeeSalaryDistribution")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val salaries = List(
        (1, "IT", 130000, "2024-01-10"),
        (2, "HR", 80000, "2024-01-15"),
        (3, "IT", 60000, "2024-02-20"),
        (4, "IT", 70000, "2024-02-25"),
        (5, "Sales", 50000, "2024-03-05"),
        (6, "IT", 90000, "2024-03-12")
    ).toDF("employee_id", "department", "salary", "last_increment_date")

    //salaries.show()

    val salaryBandDF = salaries.withColumn("salary_band", when(col("salary") > 120000, "High")
      .when((col("salary") >= 60000) && (col("salary") <= 120000), "Medium")
      .when(col("salary") < 60000, "Low"))

    salaryBandDF.show()

    salaries.filter(col("department").contains("IT")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) salary for each salary_band
    salaryBandDF.groupBy(col("salary_band")).agg(sum(col("salary")).as("total_salary"), avg(col("salary")),
      min(col("salary")), max(col("salary"))).show()

    println("=============== Spark SQL ==================")

    salaries.createOrReplaceTempView("salaries")
    salaryBandDF.createOrReplaceTempView("salary_band_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN salary > 120000 THEN 'High'
        WHEN salary BETWEEN 60000 and 120000 THEN 'Medium'
        WHEN salary < 60000 THEN 'Low'
        END AS salary_band
        FROM salaries
        """).show()

     spark.sql(
       """
         SELECT *
         FROM salaries
         WHERE department like '%IT%'
         """).show()

    spark.sql(
      """
        SELECT
        salary_band,
        sum(salary) as total_salary, avg(salary),
        min(salary), max(salary)
        FROM salary_band_tab
        GROUP BY salary_band
        """).show()
    
  }
}
