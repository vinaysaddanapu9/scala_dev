package LastSet_of_Problems

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, dayofweek}

object DailyUserEngagement {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("DailyUserEngagement")
      .master("local[*]")
      .getOrCreate()

    val engagementDF = spark.read.format("json")
      .option("multiline", true)
      .option("header", true)
      .option("inferSchema", true)
      .option("path", "E://files/engagement_data.json")
      .load()

    val usersDF = spark.read.format("json")
      .option("multiline", true)
      .option("header", true)
      .option("inferSchema", true)
      .option("path", "E://files/users_data.json")
      .load()

    //Merge daily user engagement data from multiple JSON files, group by user and week using
    //WEEK() function, calculate average engagement time, and handle nulls for missing days. Write the
    //result to Avro format

    val condition = engagementDF("user_id") === usersDF("user_id")

    val joinType = "inner"

    val joinedDF = engagementDF.join(usersDF, condition, joinType)
    val weekDF = joinedDF.select(col("*"), dayofweek(col("date")).as("week")).drop(usersDF("user_id"))

    val filledDF = weekDF.na.fill(Map("engagement_time" -> 0))

    val aggregatedDF = filledDF.groupBy(col("name"), col("week"))
      .agg(avg(col("engagement_time")).as("avg_engagement_time"))

    aggregatedDF.write.format("avro")
      .option("header", true)
      .option("mode", "overwrite")
      .option("path", "E://files/daily_user_engagement/")
      .save()

  }
}
