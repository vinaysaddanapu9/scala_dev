package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object ProjectExpenseTracking {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("ProjectExpenseTracking")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val projectExpenses = List(
        (1, "Development Project", 8000, "2024-09-01"),
        (2, "Development Plan", 4500, "2024-09-05"),
        (3, "Marketing Campaign", 2500, "2024-09-10"),
        (4, "Development Phase", 3000, "2024-09-15"),
        (5, "Development Task", 10000, "2024-09-20"),
        (6, "R&D Project", 1500, "2024-09-25")
    ).toDF("expense_id", "project_name", "expense_amount", "expense_date")

    //projectExpenses.show()
    val expenseTypeDF = projectExpenses.withColumn("expense_type", when(col("expense_amount") > 7000, "High")
      .when((col("expense_amount") >= 3000) and (col("expense_amount") <= 7000), "Medium")
      .when(col("expense_amount") < 3000, "Low"))

    expenseTypeDF.show()

    projectExpenses.filter(col("project_name").contains("Development")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //expense_amount for each expense_type
    expenseTypeDF.groupBy(col("expense_type")).agg(sum(col("expense_amount")), avg(col("expense_amount")),
      min(col("expense_amount")), max(col("expense_amount"))).show()

    println("======================= Spark SQL =========================")
    projectExpenses.createOrReplaceTempView("project_expenses")
    expenseTypeDF.createOrReplaceTempView("expense_type_tab")


    spark.sql(
      """
        SELECT *,
        CASE
        WHEN expense_amount > 7000 THEN 'High'
        WHEN expense_amount BETWEEN 3000 and 7000 THEN 'Medium'
        WHEN expense_amount < 3000 THEN 'Low'
        END AS expense_type
        FROM project_expenses
        """).show()

    spark.sql(
      """
        SELECT *
        FROM project_expenses
        WHERE project_name like '%Development%'
        """).show()

    spark.sql(
      """
        SELECT
        expense_type,
        sum(expense_amount), avg(expense_amount),
        min(expense_amount), max(expense_amount)
        FROM expense_type_tab
        GROUP BY expense_type
        """).show()

  }
}
