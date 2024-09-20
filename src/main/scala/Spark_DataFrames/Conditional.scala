package Spark_DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Conditional {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                .appName("Spark_Conditional")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._

    val employees = List((1, "John", 28),
      (2, "Jane", 35),
      (3, "Doe", 22))
     .toDF("id", "name", "age")

    employees.select(col("id"), col("name"), col("age"),
      when(col("age") >= 18, "true").otherwise("false").alias("is_adult")).show()

    //employees.show()

  }
}
