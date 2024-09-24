package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, max, min, when}

object StudentGrade {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("StudentGradeExample")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val students = List(
      (1, "Alice", 92, "Math"),
      (2, "Bob", 85, "Math"),
      (3, "Carol", 77, "Science"),
      (4, "Dave", 65, "Science"),
      (5, "Eve", 50, "Math"),
      (6, "Frank", 82, "Science")
 ).toDF("student_id", "name", "score", "subject")

    //students.show()
    val df = students.withColumn("grade", when(col("score") >= 90, "A")
      .when((col("score") >= 80) && (col("score") < 90), "B")
      .when((col("score") >= 70) && (col("score") < 80), "C")
      .when((col("score") >= 60) && (col("score") < 70), "D")
      .when(col("score") < 60, "F"))

    //Average Score Per Subject
    students.groupBy(col("subject")).agg(avg("score")).show()

    //Maximum and minimum score per subject.
    students.groupBy(col("subject")).agg(min(col("score")), max(col("score"))).show()

    //Count the number of students in each grade category per subject.
    df.groupBy(col("grade"), col("subject")).agg(count(col("student_id"))).orderBy(col("grade")).show()

    students.createOrReplaceTempView("students")
    df.createOrReplaceTempView("grade_students")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN score >= 90 THEN 'A'
        WHEN score BETWEEN 80 AND 90 THEN 'B'
        WHEN score BETWEEN 70 AND 80 THEN 'C'
        WHEN score BETWEEN 60 AND 70 THEN 'D'
        WHEN score < 60 THEN 'F'
        END AS grade
        FROM students
        """).show()

    spark.sql(
      """
        SELECT subject, avg(score),
        min(score), max(score)
        FROM students
        GROUP BY subject
        """).show()

    spark.sql(
      """
        SELECT subject, grade,
        COUNT(student_id) as no_of_students
        FROM grade_students
        GROUP BY subject, grade
        """).show()

  }
}
