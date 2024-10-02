package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object GroupEmployees {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("GroupEmployeesExample")
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

    //Calculate the lead and lag of salary within each group of employees (grouped by name) ordered by id
    val window = Window.partitionBy("name").orderBy("id")
    salary.withColumn("next_salary", lead(col("salary"), 1).over(window))
      .withColumn("previous_salary", lag(col("salary"), 1).over(window)).show()

    println("=============== Spark SQL ===================")
    salary.createOrReplaceTempView("salary")

    spark.sql(
      """
        SELECT *,
        LEAD(salary, 1) OVER(partition by name order by id) as next_salary,
        LAG(salary, 1) OVER(partition by name order by id) as previous_salary
        FROM salary
        """).show()

  }
}
