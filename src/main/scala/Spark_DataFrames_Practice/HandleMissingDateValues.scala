package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit, when}

object HandleMissingDateValues {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("HandleMissingDateValues")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = List(("2023-10-07", null), (null, "2023-10-08")).toDF("date1","date2")
    df.show()

    //Given a DataFrame with date1 and date2 columns, handle missing date values
    //by filling them with default dates.

    df.withColumn("date1", when(col("date1").isNull, lit("01-01-2024")).otherwise(col("date1")))
      .withColumn("date2", coalesce(col("date2"), lit("NA"))).show()

  }
}
