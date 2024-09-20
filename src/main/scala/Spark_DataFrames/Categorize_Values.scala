package Spark_DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Categorize_Values {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                .appName("Categorizing_Values")
                .master("local[*]").getOrCreate()

    import spark.implicits._

    val grades = List((1, 85),(2, 42),(3, 73)).toDF("student_id", "score")

    grades.select(col("student_id"), col("score"),
      when(col("score") >= 50, "Pass").otherwise("Fail").alias("grade")).show()

    //grades.show()

  }
}
