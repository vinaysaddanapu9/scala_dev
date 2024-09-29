package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dayofmonth, dayofweek, dayofyear, month, year}

object DateExtracts {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MissingDates")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df = Seq(("2023-10-07 15:30:00")).toDF("timestamp")

    val extractedDF = df.withColumn("year", year(col("timestamp")))
                        .withColumn("month", month(col("timestamp")))
                        .withColumn("day", dayofmonth(col("timestamp")))
                        .withColumn("week", dayofweek(col("timestamp")))
                        .withColumn("day_of_year", dayofyear(col("timestamp")))
    extractedDF.show()

  }
}
