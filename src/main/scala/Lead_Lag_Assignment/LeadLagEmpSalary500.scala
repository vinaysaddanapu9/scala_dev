package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object LeadLagEmpSalary500 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LeadLagEmpSalary500")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val salary = List(
      (1,"Alice", 1000),
      (2,"Bob", 2000),
      (3,"Chairs", 500),
      (4,"David", 3000),
      (5,"David", 200),
      (6,"Eliff",1000)
    ).toDF("id1","name","salary")

    //salary.show()

    //Calculate the lead and lag of the salary column for each employee, ordered by id, but only for the
    // employees who have a change in salary greater than 500 from the previous row
    val window = Window.orderBy("id1")
    val previousSalaryDF = salary.withColumn("previous_salary", lag(col("salary"), 1).over(window))

    val previousSalaryFilteredDF = previousSalaryDF.filter(col("salary") - col("previous_salary") > 500)
      .select(col("id1"), col("name"), col("salary"))

    previousSalaryFilteredDF.select(col("*"), lead(col("salary"), 1).over(window).as("next_salary"),
      lag(col("salary"), 1).over(window).as("prev_salary")).show()


  }
}
