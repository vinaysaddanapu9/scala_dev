package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lead, max, sum, when}

object SocialMediaEngagementAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("SocialMediaEngagementAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val socialMediaEngagement = List(
        ("P001", "U001", "Video", "2024-03-01", 150, 50, 20),
        ("P002", "U002", "Image", "2024-03-01", 100, 25, 15),
        ("P003", "U003", "Video", "2024-03-02", 200, 75, 30),
        ("P004", "U001", "Video", "2024-03-02", 180, 60, 25),
        ("P005", "U004", "Article", "2024-03-03", 90, 20, 10),
        ("P006", "U002", "Video", "2024-03-03", 220, 80, 40),
        ("P007", "U001", "Video", "2024-03-04", 300, 90, 50),
        ("P008", "U003", "Video", "2024-03-05", 110, 30, 10),
        ("P009", "U002", "Video", "2024-03-05", 125, 35, 15),
        ("P010", "U004", "Image", "2024-03-06", 95, 10, 5)
    ).toDF("post_id","user_id","post_type","post_date","like_count","share_count","comment_count")

    //socialMediaEngagement.show()

    val socialMediaEngagementScoreDF = socialMediaEngagement.withColumn("engagement_score",
      (col("like_count") * 0.5) + (col("share_count") * 1.0) + (col("comment_count") * 1.5))

    socialMediaEngagementScoreDF.show()

    socialMediaEngagementScoreDF.filter(col("post_type").startsWith("Video") && col("engagement_score") > 100).show()

    socialMediaEngagementScoreDF.groupBy(col("user_id"), col("post_type"))
      .agg(count(col("post_id")), sum(col("engagement_score")),
        max(col("like_count")), max(col("comment_count"))).show()

    val window = Window.partitionBy("user_id").orderBy("post_date")
    val scoresDF = socialMediaEngagementScoreDF.select(col("*"),
      lead(col("engagement_score"), 1).over(window).as("next_engagement_score"))

    scoresDF.select(col("*"),
      when(col("next_engagement_score").isNotNull && (col("next_engagement_score") > col("engagement_score")) , "Increases")
        .when(col("next_engagement_score").isNull, "Nothing")
        .otherwise("Decreases").as("engagement_score_status"))
        .show()

    println("================== Spark SQL ====================")
    socialMediaEngagement.createOrReplaceTempView("social_media_engagement")
    socialMediaEngagementScoreDF.createOrReplaceTempView("social_media_engagement_score_tab")
    scoresDF.createOrReplaceTempView("scores_tab")

    spark.sql(
      """
        SELECT *,
        (like_count * 0.5) + (share_count * 1.0) + (comment_count * 1.5)
        AS engagement_score
        FROM social_media_engagement
        """).show()

    spark.sql(
      """
        SELECT *
        FROM social_media_engagement_score_tab
        WHERE post_type like 'Video%'
        and engagement_score > 100
        """).show()

    spark.sql(
      """
        SELECT
        user_id, post_type,
        count(post_id), sum(engagement_score),
        max(like_count), max(comment_count)
        FROM social_media_engagement_score_tab
        GROUP BY user_id, post_type
        """).show()

    spark.sql(
      """
        SELECT *,
        LEAD(engagement_score, 1) OVER(partition by user_id order by post_date)
        AS next_engagement_score
        FROM social_media_engagement_score_tab
        """).show()

    spark.sql(
      """
        SELECT *,
        CASE WHEN next_engagement_score is not null AND
        (next_engagement_score > engagement_score) THEN 'Increases'
        WHEN next_engagement_score IS  NULL THEN 'Nothing'
        ELSE 'Decreases' END
        AS engagement_score_status
        FROM scores_tab
        """).show()

  }
}
