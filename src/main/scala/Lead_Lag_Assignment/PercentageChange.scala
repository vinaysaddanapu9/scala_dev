package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

object PercentageChange {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LeadLagSalary")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val salary = List(
      (1,"Alice", 1000),
      (2,"Bob", 2000),
      (3,"Charlie",1500),
      (4,"David", 3000)
    ).toDF("id1","name","salary")

    // salary.show()

    //Calculate the percentage change in salary from the previous row to the current row, ordered by id
    val window = Window.orderBy("id1")
    val salaryDiff= salary.withColumn("previous_salary", lag(col("salary"), 1)over(window))

    salaryDiff.select(col("*"),
      ((col("salary") - col("previous_salary"))/col("previous_salary") * 100.0).as("salary_diff")).show()

  }
}
