package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, date_format, lag, max, sum}

object AirlineFlightDelayAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("AirlineFlightDelayAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val flightDelays = List(
        ("F001", "Delta", "2023-11-01 08:00", "2023-11-01 10:00", 40, "New York"),
        ("F002", "United", "2023-11-01 09:00", "2023-11-01 11:30", 20, "New Orleans"),
        ("F003", "American", "2023-11-02 07:30", "2023-11-02 09:00", 60, "New York"),
        ("F004", "Delta", "2023-11-02 10:00", "2023-11-02 12:15", 30, "Chicago"),
        ("F005", "United", "2023-11-03 08:45", "2023-11-03 11:00", 50, "New York"),
        ("F006", "Delta", "2023-11-04 07:30", "2023-11-04 09:45", 70, "New York"),
        ("F007", "American", "2023-11-05 08:00", "2023-11-05 10:00", 45, "New Orleans"),
        ("F008", "United", "2023-11-06 06:00", "2023-11-06 08:30", 90, "New York"),
        ("F009", "Delta", "2023-11-07 09:30", "2023-11-07 12:00", 35, "New York"),
        ("F010", "American", "2023-11-08 08:15", "2023-11-08 10:00", 25, "New Orleans")
    ).toDF("flight_id","airline","departure_time","arrival_time","delay","destination")

    //flightDelays.show()
    val flightTimeMinsDF = flightDelays.withColumn("departure_time_mins", date_format(col("departure_time"),"mm"))
      .withColumn("arrival_time_mins", date_format(col("arrival_time"), "mm"))

    val flightDelaysMinsDF = flightTimeMinsDF.withColumn("delay_minutes",
      (col("departure_time_mins") - col("arrival_time_mins")))

    flightDelaysMinsDF.show()

    //Filter records where delay_minutes is greater than 30 and destination starts with 'New'
    flightDelaysMinsDF.filter((col("delay_minutes") > 30) && (col("destination").startsWith("New"))).show()

    //Group by airline and destination
    flightDelaysMinsDF.groupBy(col("airline"), col("destination")).agg(sum(col("delay_minutes"))
    , max(col("delay")), avg(col("delay"))).show()

    //Use the lag function to analyze the delay trend for each airline
    val window = Window.partitionBy("airline").orderBy("arrival_time")
    flightDelays.select(col("*"), lag(col("delay"), 1).over(window).as("previous_delay")).show()

    println("============= Spark SQL ===================")
    flightDelays.createOrReplaceTempView("flight_delays")
    flightTimeMinsDF.createOrReplaceTempView("flight_time_mins_tab")
    flightDelaysMinsDF.createOrReplaceTempView("flight_delays_mins_tab")

    spark.sql(
      """
        SELECT *,
        EXTRACT(minute FROM departure_time) as departure_time_mins,
        EXTRACT(minute FROM arrival_time) as arrival_time_mins
        FROM flight_delays
        """).show()

    spark.sql(
      """
        SELECT *,
        (departure_time_mins - arrival_time_mins) AS delay_minutes
        FROM flight_time_mins_tab
        """).show()

    spark.sql(
      """
        SELECT *
        FROM flight_delays_mins_tab
        WHERE delay_minutes > 30 and destination like 'New%'
        """).show()

    spark.sql(
      """
        SELECT
        airline, destination,
        sum(delay_minutes), max(delay), avg(delay)
        FROM flight_delays_mins_tab
        GROUP BY airline, destination
        """).show()

    spark.sql(
      """
        SELECT *,
        LAG(delay, 1) OVER(partition by airline order by arrival_time) as previous_delay
        FROM flight_delays
        """).show()

  }
}
