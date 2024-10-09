package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StudentCourseEnrollment {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("StudentCourseEnrollment")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val students = Seq(
      ("S001", "John"),
      ("S002", "Emma"),
      ("S003", "Olivia"),
      ("S004", "Liam"),
      ("S005", "Noah")
    ).toDF("student_id", "student_name")

    val courses = Seq(
      ("C001", "Math"),
      ("C002", "Science"),
      ("C003", "History"),
      ("C004", "Art")
    ).toDF("course_id", "course_name")

    val enrollments = Seq(
      ("E001", "S001", "C001"),
      ("E002", "S002", "C002"),
      ("E003", "S001", "C003"),
      ("E004", "S003", "C001"),
      ("E005", "S004", "C004"),
      ("E006", "S005", "C002"),
      ("E007", "S005", "C003")
    ).toDF("enrollment_id", "student_id", "course_id")

    //students.show()
    //courses.show()
    //enrollments.show()

    val condition = enrollments("course_id") === courses("course_id")

    val condition2 = enrollments("student_id") === students("student_id")

    val joinType = "inner"

    val joined_df = enrollments.join(courses, condition, joinType)
      .join(students, condition2, joinType)
      .drop(courses("course_id"))
      .drop(students("student_id"))

    joined_df.show()

    println("================ Spark SQL ==================")
    enrollments.createOrReplaceTempView("enrollments")
    students.createOrReplaceTempView("students")
    courses.createOrReplaceTempView("courses")

    spark.sql(
      """
        SELECT *
        FROM enrollments e
        JOIN courses c ON
        e.course_id = c.course_id
        JOIN students s ON
        e.student_id = s.student_id
        """).show()

  }
}
