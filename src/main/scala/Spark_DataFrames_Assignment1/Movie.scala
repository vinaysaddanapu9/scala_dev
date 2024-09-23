package Spark_DataFrames_Assignment1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count}

object Movie {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MovieExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ratingsData = Seq(
      ("User1", "Movie1", 4.5),
      ("User2", "Movie1", 3.5),
      ("User3", "Movie2", 2.5),
      ("User4", "Movie2", 3.0),
      ("User1", "Movie3", 5.0),
      ("User2", "Movie3", 4.0)
    ).toDF("User", "Movie","Rating")

    //Average rating for each movie and the total number of ratings for each movie.
    ratingsData.groupBy(col("Movie")).agg(avg(col("Rating")).as("Average_Rating"),
      count(col("Rating")).as("total no.of ratings")).orderBy("Movie").show()

    //ratingsData.show()

  }
}
