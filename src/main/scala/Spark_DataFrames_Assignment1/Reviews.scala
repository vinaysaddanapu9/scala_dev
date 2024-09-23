package Spark_DataFrames_Assignment1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Reviews {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("ReviewsExample")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._

    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5)
    ).toDF("review_id", "rating")

    reviews.withColumn("feedback", when(col("rating") < 3, "Bad")
      .when((col("rating") === 3) || (col("rating") === 4),"Good")
      .when(col("rating") === 5, "Excellent"))
      .withColumn("is_positive", when(col("rating") >= 3, true).otherwise(false)).show()

    reviews.createOrReplaceTempView("reviews")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN rating < 3 THEN 'Bad'
        WHEN rating = 3 or rating = 4 THEN 'Good'
        WHEN rating = 5 THEN 'Excellent'
        END AS feedback,
        CASE WHEN rating >= 3 THEN true ELSE false END AS is_positive
        FROM reviews
        """).show()

    //reviews.show()






  }
}
