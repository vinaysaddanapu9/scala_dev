package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, min}

object SalaryDifferenceLastThreeRows {

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

    salary.show()

    //Calculate the difference between the current salary and the minimum salary within the last three rows,
    //ordered by id
    val window = Window.orderBy("id1")
    //val salaryDiffDF = salary.withColumn("salary_last_3_rows", lag(col("salary"), 3).over(window))
    //salaryDiffDF.withColumn("salary_diff", col("salary") - col("salary_last_3_rows")).show()

    val window2 = Window.orderBy("id1").rowsBetween(-2, 0)
    val salaryDiffDF = salary.withColumn("min_salary_last_3_rows", min(col("salary")).over(window2))
    salaryDiffDF.withColumn("salary_diff", col("salary") - col("min_salary_last_3_rows")).show()

  }
}
