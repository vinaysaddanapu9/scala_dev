package Spark_DataFrames_Assignment1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Weather_1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
               .appName("WeatherExample")
               .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val weather = Seq(
      (1, 25, 60),
      (2, 35, 40),
      (3, 15, 80)
    ).toDF("day_id", "temperature", "humidity")

    weather.withColumn("is_hot", when(col("temperature") > 30, true).otherwise(false))
      .withColumn("is_humid", when(col("humidity") > 40, true).otherwise(false)).show()

    weather.createOrReplaceTempView("weather")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN temperature > 30 THEN true ELSE false END AS is_hot,
        CASE
        WHEN humidity > 40 THEN true ELSE false END AS is_humid
        FROM weather
        """).show()

    //weather.show()

  }
}
