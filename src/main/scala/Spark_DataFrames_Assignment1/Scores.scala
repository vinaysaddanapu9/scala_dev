package Spark_DataFrames_Assignment1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Scores {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("ScoresExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val scores = List(
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)
    ).toDF("student_id", "math_score", "english_score")

    scores.withColumn("math_grade", when(col("math_score") > 80, "A")
      .when((col("math_score") > 60) && (col("math_score") <= 80), "B").otherwise("C"))
      .withColumn("english_grade", when(col("english_score") >  80, "A")
        .when((col("english_score") > 60) && (col("english_score") <= 80), "B").otherwise("C")).show()

    scores.createOrReplaceTempView("scores")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN math_score > 80 THEN 'A'
        WHEN math_score > 60 and math_score <= 80 THEN 'B'
        ELSE 'C'
        END AS math_grade,
        CASE
        WHEN english_score > 80 THEN 'A'
        WHEN english_score > 60 and english_score <= 80 THEN 'B'
        ELSE 'C'
        END AS english_grade
        FROM scores
        """).show()

    //scores.show()

  }
}
