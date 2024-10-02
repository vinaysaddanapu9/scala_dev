package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum}

object RunningTotal {

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

    //salary.show()

    //Running total of salary for each employee ordered by id
    val window = Window.orderBy("id1")
    salary.withColumn("Running_Total", sum(col("salary")).over(window)).show()

  }
}
