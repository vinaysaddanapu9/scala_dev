package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

object PreviousDayPriceDifference {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Practice2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val prices = List(
        (1, "KitKat",1000.0, "2021-01-01"),
       (1, "KitKat",2000.0, "2021-01-02"),
       (1, "KitKat",1000.0, "2021-01-03"),
       (1, "KitKat",2000.0,"2021-01-04"),
       (1, "KitKat",3000.0,"2021-01-05"),
       (1, "KitKat",1000.0,"2021-01-06")
    ).toDF("IT_ID","IT_Name","Price","PriceDate")

    prices.show()

    //we want to find the difference between the price on each day with it’s previous day.
     val window = Window.orderBy("PriceDate")

    prices.select(col("IT_ID"), col("IT_Name"), col("Price"), col("PriceDate"),
       (col("Price") - lag(col("Price"), 1).over(window)).as("prev_day_price_diff")).show()

  }
}
