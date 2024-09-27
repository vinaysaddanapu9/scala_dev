package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object EmployeeBonusCalculation {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("EmployeeBonusCalculation")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val employeeBonuses = List(
        (1, "Sales Department", 2500, "2024-01-10"),
        (2, "Marketing Department", 1500, "2024-01-15"),
        (3, "IT Department", 800, "2024-01-20"),
        (4, "HR Department", 1200, "2024-02-01"),
        (5, "Sales Department", 1800, "2024-02-10"),
        (6, "IT Department", 950, "2024-03-01")
    ).toDF("employee_id", "department", "bonus", "bonus_date")

    //employeeBonuses.show()
    val employeeBonusCategoryDF = employeeBonuses.withColumn("bonus_category", when(col("bonus") > 2000, "High")
      .when((col("bonus") >= 1000) and (col("bonus") <= 2000), "Medium")
      .when(col("bonus") < 1000, "Low"))

    employeeBonusCategoryDF.show()

    employeeBonuses.filter(col("department").endsWith("Department")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) bonus for each bonus_category
    employeeBonusCategoryDF.groupBy(col("bonus_category")).agg(sum(col("bonus")).as("total_bonus"), avg(col("bonus")),
      min(col("bonus")), max(col("bonus"))).show()

    println("============== Spark SQL ==================")
    employeeBonuses.createOrReplaceTempView("employee_bonuses")
    employeeBonusCategoryDF.createOrReplaceTempView("employee_bonuses_category_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN bonus > 2000 THEN 'High'
        WHEN bonus BETWEEN 1000 and 2000 THEN 'Medium'
        WHEN bonus < 1000 THEN 'Low'
        END AS bonus_category
        FROM employee_bonuses
        """).show()

    spark.sql(
      """
        SELECT *
        FROM
        employee_bonuses
        WHERE department like '%Department'
        """).show()

    spark.sql(
      """
        SELECT
        bonus_category,
        sum(bonus) as total_bonus, avg(bonus),
        min(bonus), max(bonus)
        FROM employee_bonuses_category_tab
        GROUP BY bonus_category
        """).show()

  }
}
