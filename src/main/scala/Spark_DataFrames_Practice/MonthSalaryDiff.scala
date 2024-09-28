package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lag, when}
import org.apache.spark.sql.expressions.Window

object MonthSalaryDiff {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Practice2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val salaries = List(
        (1,"John",1000,"01/01/2016"),
        (1,"John",2000,"02/01/2016"),
        (1,"John",1000,"03/01/2016"),
        (1,"John",2000,"04/01/2016"),
        (1,"John",3000,"05/01/2016"),
        (1,"John",1000,"06/01/2016")
    ).toDF("ID","NAME","SALARY","DATE")

    salaries.show()

    //If salary is less than previous month we will mark it as "DOWN", if salary has increased then "UP"
    val window = Window.orderBy("date")

    val df2 = salaries.withColumn("previous_salary", lag(col("SAlARY"), 1).over(window))
    //df2.show()

    df2.withColumn("salary_difference", when(col("salary") < col("previous_salary"), "DOWN")
      .when(col("salary") > col("previous_salary"), "UP").otherwise("No Change")).show()
  }

}
