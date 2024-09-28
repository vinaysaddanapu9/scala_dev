package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object LibraryBookLoans {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LibraryBookLoans")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val bookLoans = List(
        (1, "History of Rome", 40, "2024-01-10"),
        (2, "Modern History", 20, "2024-01-15"),
        (3, "Ancient History", 10, "2024-02-20"),
        (4, "European History", 15, "2024-02-25"),
        (5, "World History", 5, "2024-03-05"),
        (6, "History of Art", 35, "2024-03-12")
    ).toDF("loan_id", "book_title", "loan_duration_days", "loan_date")

    //bookLoans.show()
    val loanCategoryDF = bookLoans.withColumn("loan_category", when(col("loan_duration_days") > 30, "Long-Term")
      .when((col("loan_duration_days") >= 15) and (col("loan_duration_days") <= 30), "Medium-Term")
      .when(col("loan_duration_days") < 15, "Short-Term"))

    loanCategoryDF.show()

    bookLoans.filter(col("book_title").contains("History")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //loan_duration_days for each loan_category
    loanCategoryDF.groupBy(col("loan_category")).agg(sum(col("loan_duration_days")), avg(col("loan_duration_days")),
      min(col("loan_duration_days")), max(col("loan_duration_days"))).show()

    println("=========== Spark SQL =================")
    bookLoans.createOrReplaceTempView("book_loans")
    loanCategoryDF.createOrReplaceTempView("loan_category_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN loan_duration_days > 30 THEN 'Long-Term'
        WHEN loan_duration_days BETWEEN 15 and 30 THEN 'Medium-Term'
        WHEN loan_duration_days < 15 THEN 'Short-Term'
        END AS loan_category
        FROM book_loans
        """).show()

     spark.sql(
       """
         SELECT *
         FROM book_loans
         WHERE book_title like '%History%'
         """).show()

    spark.sql(
      """
        SELECT
        loan_category,
        sum(loan_duration_days), avg(loan_duration_days),
        min(loan_duration_days), max(loan_duration_days)
        FROM loan_category_tab
        GROUP BY loan_category
        """).show()

  }
}
