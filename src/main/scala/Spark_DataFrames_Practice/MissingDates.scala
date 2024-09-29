package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit, when}

object MissingDates {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MissingDates")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df = Seq(("2023-10-07", null), (null, "2023-10-08")).toDF("date1", "date2")

    df.withColumn("date1", when(col("date1").isNull, lit("01-01-2024")).otherwise(col("date1")))
      .withColumn("date2", coalesce(col("date2"), lit("NA")))
      .show()

    df.filter(col("date1").isNotNull).show()

  }
}
