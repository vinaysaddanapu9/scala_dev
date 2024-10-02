package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object LeadLagEmpSalary1500 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("LeadLagEmpSalary1500")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val salary = List(
      (1,"Alice", 1000),
      (2,"Bob", 2000),
      (3,"Charlie",1500),
      (4,"David", 3000)
    ).toDF("id1","name","salary")

    //Calculate the lead and lag of the salary column for each employee ordered by id, but only for the
    //employees who have a salary greater than 1500

    val filteredEmpSalary = salary.filter(col("salary") > 1500)
    filteredEmpSalary.show()

    val window = Window.orderBy("id1")
    filteredEmpSalary.withColumn("next_salary", lead(col("salary"), 1).over(window))
      .withColumn("previous_salary", lag(col("salary"), 1).over(window)).show()


  }
}
