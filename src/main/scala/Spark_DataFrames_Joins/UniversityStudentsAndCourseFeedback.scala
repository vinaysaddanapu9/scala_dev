package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object UniversityStudentsAndCourseFeedback {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("UniversityStudentsAndCourseFeedback")
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

    val feedback = Seq(
      ("F001", "S001", "C001", 4),
      ("F002", "S002", "C002", 5),
      ("F003", "S001", "C003", 3),
      ("F004", "S004", "C004", 5),
      ("F005", "S003", "C001", 4),
      ("F006", "S003", "C002", 2)
    ).toDF("feedback_id", "student_id", "course_id", "rating")

    val condition = feedback("course_id") === courses("course_id")

    val condition2 = feedback("student_id") === students("student_id")

    val condition3 = courses("instructor_id") === instructors("instructor_id")

    val joinType = "inner"

    val joined_df = feedback.join(courses, condition, joinType)
      .join(students, condition2, joinType)
      .join(instructors, condition3, joinType)
      .drop(courses("instructor_id"))

      joined_df.show()

      println("================= Spark SQL =================")
      students.createOrReplaceTempView("students")
      courses.createOrReplaceTempView("courses")
      instructors.createOrReplaceTempView("instructors")
      feedback.createOrReplaceTempView("feedback")

     spark.sql(
       """
         SELECT *
         FROM feedback f
         JOIN courses c ON
         f.course_id = c.course_id
         JOIN students s ON
         f.student_id = s.student_id
         JOIN instructors i ON
         c.instructor_id = i.instructor_id
         """).show()

  }
}
