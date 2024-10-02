package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object LeadLagSalary {

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

    //salary.show()

    //Calculate the lead and lag of the salary column ordered by id
    val window = Window.orderBy("id1")

    salary.select(col("*"),
      lead(col("salary"), 1).over(window).as("next_salary"),
      lag(col("salary"), 1).over(window).as("previous_salary"))
      .show()

  }
}
