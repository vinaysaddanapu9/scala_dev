package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object CorporateTrainingExpenses {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("CorporateTrainingExpenses")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val trainingExpenses = List(
        (1, "HR", 3500, "2024-07-01"),
        (2, "IT", 1200, "2024-07-05"),
        (3, "HR", 600, "2024-07-10"),
        (4, "HR", 2500, "2024-07-15"),
        (5, "IT", 800, "2024-07-20"),
        (6, "HR", 4000, "2024-07-25")
    ).toDF("expense_id", "department", "expense_amount", "expense_date")

    //trainingExpenses.show()

    val expenseCategory = trainingExpenses.withColumn("expense_category", when(col("expense_amount") > 3000, "High")
      .when((col("expense_amount") >= 1000) and (col("expense_amount") <= 3000), "Medium")
      .when(col("expense_amount") < 1000, "Low"))

    expenseCategory .show()

    trainingExpenses.filter(col("department").startsWith("HR")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //expense_amount for each expense_category
    expenseCategory.groupBy(col("expense_category")).agg(sum(col("expense_amount")), avg(col("expense_amount")),
      min(col("expense_amount")), max(col("expense_amount"))).show()

    println("================= Spark SQL ==================")
    trainingExpenses.createOrReplaceTempView("training_expenses")
    expenseCategory.createOrReplaceTempView("expense_category_tab")

   spark.sql(
     """
       SELECT *,
       CASE
       WHEN expense_amount > 3000 THEN 'High'
       WHEN expense_amount BETWEEN 1000 and 3000 THEN 'Medium'
       WHEN expense_amount < 1000 THEN 'Low'
       END AS expense_category
       FROM training_expenses
       """).show()

    spark.sql(
      """
        SELECT *
        FROM training_expenses
        WHERE department like 'HR%'
        """).show()

    spark.sql(
      """
        SELECT
        expense_category,
        sum(expense_amount), avg(expense_amount),
        min(expense_amount), max(expense_amount)
        FROM expense_category_tab
        GROUP BY expense_category
        """).show()

  }
}
