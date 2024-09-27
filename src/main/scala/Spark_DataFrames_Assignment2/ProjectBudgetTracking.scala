package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object ProjectBudgetTracking {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

   val spark = SparkSession.builder()
               .appName("ProjectBudgetTracking")
               .master("local[*]")
               .getOrCreate()

    import spark.implicits._
    val projectBudgets = List(
        (1, "New Website", 50000, 55000),
        (2, "Old Software", 30000, 25000),
        (3, "New App", 40000, 40000),
        (4, "New Marketing", 15000, 10000),
        (5, "Old Campaign", 20000, 18000),
        (6, "New Research", 60000, 70000)
    ).toDF("project_id", "project_name", "budget", "spent_amount")

    //projectBudgets.show()
    val projectBudgetStatusDF = projectBudgets.withColumn("budget_status",
      when(col("spent_amount") > col("budget"), "Over Budget")
      .when(col("spent_amount") === col("budget"), "On Budget")
      .when(col("spent_amount") < col("budget"), "Under Budget"))

    projectBudgetStatusDF.show()

    projectBudgets.filter(col("project_name").startsWith("New")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) spent_amount
    //for each budget_status
    projectBudgetStatusDF.groupBy(col("budget_status")).agg(sum(col("spent_amount")).as("total_spent_amount"),
      avg(col("spent_amount")),min(col("spent_amount")), max(col("spent_amount"))).show()

    println("============== Spark SQL ===============")
    projectBudgets.createOrReplaceTempView("project_budgets")
    projectBudgetStatusDF.createOrReplaceTempView("project_budgets_status_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN spent_amount > budget THEN 'Over Budget'
        WHEN spent_amount = budget THEN 'On Budget'
        WHEN spent_amount < budget THEN 'Under Budget'
        END AS budget_status
        FROM project_budgets
        """).show()

    spark.sql(
      """
        SELECT *
        FROM project_budgets
        WHERE project_name like 'New%'
        """).show()

    spark.sql(
      """
        SELECT
        budget_status,
        sum(spent_amount) as total_spent_amount, avg(spent_amount),
        min(spent_amount), max(spent_amount)
        FROM project_budgets_status_tab
        GROUP BY budget_status
        """).show()

  }
}
