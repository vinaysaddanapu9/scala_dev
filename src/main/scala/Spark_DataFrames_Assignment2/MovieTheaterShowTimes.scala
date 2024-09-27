package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object MovieTheaterShowTimes {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("MovieTheaterShowTimes")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val  showTimes = List(
        (1, "Action Hero", "2024-01-10", 8),
        (2, "Comedy Nights", "2024-01-15", 25),
        (3, "Action Packed", "2024-01-20", 55),
        (4, "Romance Special", "2024-02-01", 5),
        (5, "Action Force", "2024-02-10", 45),
        (6, "Drama Series", "2024-03-01", 70)
    ).toDF("show_id", "movie_title","showtime", "seats_available")

    //showTimes.show()
    val seatsAvailabilityDF = showTimes.withColumn("availability", when(col("seats_available") <= 10, "Full")
      .when((col("seats_available") >= 11) && (col("seats_available") <= 50), "Limited")
      .when(col("seats_available") > 50, "Plenty"))

    seatsAvailabilityDF.show()

    showTimes.filter(col("movie_title").contains("Action")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //seats_available for each availability
    seatsAvailabilityDF.groupBy(col("availability")).agg(sum(col("seats_available")).as("total_sum"),
      avg(col("seats_available")), min(col("seats_available")),
     max(col("seats_available"))).show()

    println("============ Spark SQL ====================")

    showTimes.createOrReplaceTempView("showtimes")
    seatsAvailabilityDF.createOrReplaceTempView("seats_availability_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN seats_available <= 10 THEN 'Full'
        WHEN seats_available BETWEEN 11 and 50 THEN 'Limited'
        WHEN seats_available > 50 THEN 'Plenty'
        END AS availability
        FROM showtimes
        """).show()

     spark.sql(
       """
         SELECT *
         FROM showtimes
         WHERE movie_title like '%Action%'
         """).show()

    spark.sql(
      """
        SELECT
        availability,
        sum(seats_available) as total_sum, avg(seats_available),
        min(seats_available), max(seats_available)
        FROM seats_availability_tab
        GROUP BY availability
        """).show()

  }
}
