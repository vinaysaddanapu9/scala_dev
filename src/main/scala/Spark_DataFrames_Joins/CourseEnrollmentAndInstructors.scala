package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CourseEnrollmentAndInstructors {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("CourseEnrollmentAndInstructors")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val students = Seq(
      ("S001", "John"),
      ("S002", "Emma"),
      ("S003", "Olivia"),
      ("S004", "Liam")
    ).toDF("student_id", "student_name")

    val courses = Seq(
      ("C001", "Math", "I001"),
      ("C002", "Science", "I002"),
      ("C003", "History", "I001"),
      ("C004", "Art", "I003")
    ).toDF("course_id", "course_name", "instructor_id")

    val instructors = Seq(
      ("I001", "Prof. Brown"),
      ("I002", "Prof. Green"),
      ("I003", "Prof. White")
    ).toDF("instructor_id", "instructor_name")

    val enrollments = Seq(
      ("E001", "S001", "C001"),
      ("E002", "S002", "C002"),
      ("E003", "S001", "C003"),
      ("E004", "S004", "C004"),
      ("E005", "S003", "C001"),
      ("E006", "S003", "C002")
    ).toDF("enrollment_id", "student_id", "course_id")

    val condition = enrollments("student_id") === students("student_id")

    val condition2 = enrollments("course_id") === courses("course_id")

    val condition3 = courses("instructor_id") === instructors("instructor_id")

    val joinType = "inner"

    val joined_df = enrollments.join(students, condition, joinType)
      .join(courses, condition2, joinType)
      .join(instructors, condition3, joinType)
      .drop(courses("instructor_id"))

    joined_df.show()

    println("================ Spark SQL ========================")
    students.createOrReplaceTempView("students")
    courses.createOrReplaceTempView("courses")
    instructors.createOrReplaceTempView("instructors")
    enrollments.createOrReplaceTempView("enrollments")

    spark.sql(
      """
        SELECT *
        FROM enrollments e
        JOIN students s ON
        e.student_id = s.student_id
        JOIN courses c ON
        e.course_id = c.course_id
        JOIN instructors i ON
        c.instructor_id = i.instructor_id
        """).show()

  }
}
