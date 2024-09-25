package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object MovieRatingDurationAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MovieRatingAndDurationAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val movies = List(
      (1, "The Matrix", 9, 136),
      (2, "Inception", 8, 148),
      (3, "The Godfather", 9, 175),
      (4, "Toy Story", 7, 81),
      (5, "The Shawshank Redemption", 10, 142),
      (6, "The Silence of the Lambs", 8, 118)
    ).toDF("movie_id", "movie_name", "rating", "duration_minutes")

    val df1 = movies.select(col("*"), when(col("rating") >= 8, "Excellent")
      .when((col("rating") >= 6) && (col("rating") < 8), "Good").when(col("rating") < 6, "Average")
      .as("rating_category"))

    df1.show()

    movies.select(col("*"), when(col("duration_minutes") > 150, "Long")
      .when((col("duration_minutes") >= 90) && (col("duration_minutes") <= 150), "Medium")
      .when(col("duration_minutes") < 90, "Short").as("duration_category")).show()

    //Filter movies
    movies.filter(col("movie_name").startsWith("T")).show()
    movies.filter(col("movie_name").endsWith("e")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //duration_minutes for each rating_category

    df1.groupBy(col("rating_category")).agg(sum(col("duration_minutes")).as("total_sum"), avg(col("duration_minutes")),
      min(col("duration_minutes")), max(col("duration_minutes"))).show()

    println("=========== Spark SQL ==================")

    movies.createOrReplaceTempView("movies")
    df1.createOrReplaceTempView("movies_rating_category")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN rating >= 8 THEN 'Excellent'
        WHEN rating BETWEEN 6 and 8 THEN 'Good'
        WHEN rating < 6 THEN 'Average'
        END AS rating_category
        FROM movies
        """).show()

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN duration_minutes > 150 THEN 'Long'
        WHEN duration_minutes BETWEEN 90 and 150 THEN 'Medium'
        WHEN duration_minutes < 90 THEN 'Short'
        END AS duration_category
        FROM movies
        """).show()

    spark.sql(
      """
        SELECT *
        FROM movies
        WHERE movie_name like 'T%'
        """).show()

    spark.sql(
      """
        SELECT *
        FROM movies
        WHERE movie_name like '%e'
        """).show()

    spark.sql(
      """
        SELECT
        rating_category,
        sum(duration_minutes) as total_sum, avg(duration_minutes),
        min(duration_minutes), max(duration_minutes)
        FROM movies_rating_category
        GROUP BY rating_category
        """).show()

    //movies.show()

  }
}
