package Spark_DataFrames_Assignment1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Events {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("EventsExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val events = List(
      (1, "2024-07-27"),
      (2, "2024-12-25"),
      (3, "2025-01-01")
    ).toDF("event_id", "date")

    events.withColumn("is_holiday",
      when((col("date") === "2024-12-25") || (col("date") === "2025-01-01"), true).otherwise(false)).show()

    //events.show()

  }
}
