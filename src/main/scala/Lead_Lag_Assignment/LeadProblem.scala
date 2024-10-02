package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead}

object LeadProblem {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LeadProblem")
      .master("local[*]")
      .getOrCreate()

    //LEAD AND LAG
    import spark.implicits._
    val customers = List(
      (101,"CustomerA","2023-09-01"),
      (103,"CustomerA","2023-09-03"),
      (102,"CustomerB","2023-09-02"),
      (104,"CustomerB","2023-09-04")
   ).toDF("order_id", "customer","order_date")

    //customers.show()

    val window = Window.orderBy("order_date")

    customers.select(col("order_id"), col("customer"), col("order_date"),
      lead(col("order_date"), 1).over(window).as("next_date")).show()





  }
}
