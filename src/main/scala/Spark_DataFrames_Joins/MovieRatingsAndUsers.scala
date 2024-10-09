package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col}

object MovieRatingsAndUsers {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MovieRatingsAndUsers")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val movies = Seq(
      ("M001", "Inception"),
      ("M002", "The Dark Knight"),
      ("M003", "Interstellar"),
      ("M004", "Titanic")
    ).toDF("movie_id", "movie_title")

    val users = Seq(
      ("U001", "Alice"),
      ("U002", "Bob"),
      ("U003", "Charlie")
    ).toDF("user_id", "user_name")

    val ratings = Seq(
      ("R001", "M001", "U001", 5),
      ("R002", "M002", "U001", 4),
      ("R003", "M003", "U002", 5),
      ("R004", "M002", "U003", 3),
      ("R005", "M001", "U002", 4),
      ("R006", "M004", "U001", 2)
    ).toDF("rating_id", "movie_id", "user_id", "rating")

    //movies.show()
    //users.show()
    //ratings.show()

    val condition = ratings("movie_id") === movies("movie_id")

    val condition2 = ratings("user_id") === users("user_id")

    val joinType = "inner"

    val joined_df  = ratings.join(movies, condition, joinType)
                            .join(users, condition2, joinType)

    joined_df.show()

    val avg_rating_df = joined_df.groupBy(col("movie_title")).agg(avg(col("rating")))
    avg_rating_df.show()

  }
}
