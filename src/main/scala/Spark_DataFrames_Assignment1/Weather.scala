package Spark_DataFrames_Assignment1

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{avg, col, min, max}

object Weather {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

   val spark = SparkSession.builder()
     .appName("WeatherExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val weatherData = Seq(
      ("City1", "2022-01-01", 10.0),
      ("City1", "2022-01-02", 8.5),
      ("City1", "2022-01-03", 12.3),
      ("City2", "2022-01-01", 15.2),
      ("City2", "2022-01-02", 14.1),
      ("City2", "2022-01-03", 16.8)
   ).toDF("City", "Date","Temperature")

    //Find the minimum, maximum, and average temperature for each city
    weatherData.groupBy(col("City")).agg(min(col("Temperature")).as("min_temperature"),
      max(col("Temperature")).as("max_temperature"), avg(col("Temperature")).as("avg_temperature")).show()

    //weatherData.show()
  }
}
