package WindowAggregationProblem

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, desc}

object UserAverageRating {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("UserAverageRating")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ratingData = Seq(
      ("User1", "Movie1", 4.5),
      ("User1", "Movie2", 3.5),
      ("User1", "Movie3", 2.5),
      ("User1", "Movie4", 4.0),
      ("User1", "Movie5", 3.0),
      ("User1", "Movie6", 4.5),
      ("User2", "Movie1", 3.0),
      ("User2", "Movie2", 4.0),
      ("User2", "Movie3", 4.5),
      ("User2", "Movie4", 3.5),
      ("User2", "Movie5", 4.0),
      ("User2", "Movie6", 3.5)
    ).toDF("User", "Movie", "Rating")

    //ratingData.show()

    //Calculate the average rating for each user based on the last 3 ratings

    val window = Window.partitionBy("User").orderBy(desc("Movie")).rowsBetween(-2, 0)

    val averageRating = ratingData.withColumn("AverageRating", avg("Rating").over(window))

    averageRating.show()

  }
}
