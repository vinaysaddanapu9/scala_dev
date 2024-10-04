package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lead, sum, when, year}

object OnlineCourseCompletionAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("OnlineCourseCompletionAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val courseCompletionData = List(
        ("C001", "S001", "Data Science", "2023-11-01", 90, true),
        ("C002", "S002", "Web Development", "2023-12-01", 80, false),
        ("C003", "S003", "Data Science", "2023-10-15", 85, true),
        ("C004", "S001", "Machine Learning", "2023-11-20", 88, true),
        ("C005", "S005", "Web Development", "2024-01-01", 92, true),
        ("C006", "S002", "Data Science", "2024-01-15", 95, true),
        ("C007", "S003", "Machine Learning", "2024-02-01", 91, true),
        ("C008", "S004", "Data Science", "2023-11-10", 87, true),
        ("C009", "S005", "Web Development", "2024-01-20", 89, true),
        ("C010", "S001", "Web Development", "2024-02-05", 84, true)
    ).toDF("completion_id","student_id","course_name","completion_date","score","certificate_awarded")

    //courseCompletionData.show()

    val courseCompletionYearDF = courseCompletionData.withColumn("completion_year", year(col("completion_date")))

    courseCompletionYearDF.show()

    courseCompletionData.filter(col("score") > 85 && col("certificate_awarded") === true).show()

    courseCompletionYearDF.groupBy(col("course_name"), col("completion_year"))
      .agg(count(when(col("student_id").isNotNull, 1)).as("total_students"),
        avg(col("score")),
        count(when(col("certificate_awarded") === true, 1)).as("count_certificates_awarded"))
      .show()

    //Use the lead function to predict the next course a student might complete based on their
    //past completions
    val window = Window.orderBy("student_id", "completion_id")
    courseCompletionData.select(col("*"),
        lead(col("course_name"), 1).over(window).as("next_course")).show()

    println("================ Spark SQL ==================")
    courseCompletionData.createOrReplaceTempView("course_completion")
    courseCompletionYearDF.createOrReplaceTempView("course_completion_year_tab")

    spark.sql(
      """
        SELECT *,
        EXTRACT(year FROM completion_date) AS completion_year
        FROM course_completion
        """).show()

    spark.sql(
      """
        SELECT *
        FROM course_completion
        WHERE score > 85 and certificate_awarded = true
        """).show()

    spark.sql(
      """
        SELECT
        course_name, completion_year,
        count(CASE WHEN student_id IS NOT NULL THEN 1 END) as total_students,
        avg(score),
        count(CASE WHEN certificate_awarded = true THEN 1 END) as count_certificates_awarded
        FROM course_completion_year_tab
        GROUP BY course_name, completion_year
        """).show()

    spark.sql(
      """
        SELECT *,
        LEAD(course_name, 1) OVER(order by student_id, completion_id) as next_course
        FROM course_completion
        """).show()

  }
}
