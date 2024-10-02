package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max}

object MaximumSalary {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MaximumSalaryExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val salary = List(
      (1,	"Alice",50000),
      (2,	"Alice",55000),
      (3,	"Bob",48000),
      (4,	"Bob",52000),
      (5,	"Charlie",60000),
      (6,	"Charlie",62000),
      (7,	"Alice",57000),
      (8,	"Bob",53000)
    ).toDF("id","name","salary")

    salary.show()

    //Find the maximum salary for each employee’s group (partitioned by name) and display it for each row
    val window = Window.partitionBy("name")
    salary.withColumn("max_salary", max(col("salary")).over(window)).show()

    println("================ Spark SQL ==================")
    salary.createOrReplaceTempView("salary")

    spark.sql(
      """
        SELECT *,
        MAX(salary) OVER(partition by name) as max_salary
        FROM salary
        """).show()


  }
}
