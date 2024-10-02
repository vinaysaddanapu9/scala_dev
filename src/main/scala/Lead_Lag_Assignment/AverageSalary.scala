package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col}

object AverageSalary {

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

    //salary.show()

    //Calculate the difference between the current salary and the average salary for each employee’s group
    //(partitioned by name) ordered by id
    val window = Window.partitionBy("name")
    val averageSalaryDF = salary.withColumn("average_salary", avg(col("salary")).over(window))

    averageSalaryDF.select(col("*"), (col("salary") - col("average_salary")).as("avg_salary_diff"))
      .orderBy(col("name"), col("id")).show()

    println("=============== Spark SQL ==================")
    salary.createOrReplaceTempView("salary")
    averageSalaryDF.createOrReplaceTempView("average_salary_tab")

    spark.sql(
      """
        SELECT *,
        AVG(salary) OVER(partition by name) AS average_salary
        FROM salary
        """).show()

    spark.sql(
      """
        SELECT *,
        salary - average_salary AS avg_salary_diff
        FROM average_salary_tab
        ORDER BY name, id
        """).show()

  }
}
