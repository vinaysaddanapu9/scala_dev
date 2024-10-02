package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, rank}

object Rank {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("RankExample")
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
    //Calculate the rank of each employee based on their salary, ordered by salary in descending order

    val window = Window.orderBy(desc("salary"))
    salary.select(col("*"), rank().over(window).as("rank")).show()

    println("================ Spark SQL ======================")
    salary.createOrReplaceTempView("salary")

    spark.sql(
      """
        SELECT *,
        RANK() OVER(order by salary desc) as rank
        FROM salary
        """).show()

  }
}
