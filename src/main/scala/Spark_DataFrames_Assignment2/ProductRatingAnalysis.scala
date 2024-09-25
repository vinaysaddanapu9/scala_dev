package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, when}

object ProductRatingAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
              .appName("ProductRatingAnalysis")
              .master("local[*]")
              .getOrCreate()

    import spark.implicits._
    val productReviews = List(
      (1, "Smartphone", 4, "2024-01-15"),
      (2, "Speaker", 3, "2024-01-20"),
      (3, "Smartwatch", 5, "2024-02-15"),
      (4, "Screen", 2, "2024-02-20"),
      (5, "Speakers", 4, "2024-03-05"),
      (6, "Soundbar", 3, "2024-03-12")
    ).toDF("review_id", "product_name", "rating", "review_date")

    //productReviews.show()
    val rating_category_df = productReviews.withColumn("rating_category", when(col("rating") >= 4, "High")
      .when((col("rating") >= 3) && (col("rating") < 4), "Medium").when(col("rating") < 3, "Low"))

    rating_category_df.show()

    productReviews.filter(col("product_name").startsWith("S")).show()

    //Calculate the total count of reviews and average rating for each rating_category
    rating_category_df.groupBy(col("rating_category")).agg(count(col("review_id")), avg(col("rating"))).show()

    println("==============Spark SQL ==================")

    productReviews.createOrReplaceTempView("product_reviews")
    rating_category_df.createOrReplaceTempView("rating_category_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN rating >= 4 THEN 'High'
        WHEN rating BETWEEN 3 and 4 THEN 'Medium'
        WHEN rating < 3 THEN 'Low'
        END AS rating_category
        FROM product_reviews
        """).show()

    spark.sql(
      """
        SELECT *
        FROM product_reviews
        WHERE product_name like 'S%'
        """).show()

    spark.sql(
      """
        SELECT
        rating_category,
        COUNT(review_id) as total_reviews, avg(rating)
        FROM rating_category_tab
        GROUP BY rating_category
        """).show()

  }
}
