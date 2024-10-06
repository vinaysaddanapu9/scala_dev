package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{add_months, col, date_add, date_sub, to_date, to_timestamp}

object DateArithmeticFunctions {

  def main(args:Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("DateArithmeticFunctions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df = List(("2023-10-07", "15:30:00")).toDF("date_str", "time_str")

    //Convert date string to date, time string to time
    val formattedDF = df.withColumn("date", to_date(col("date_str")))
      .withColumn("time", to_timestamp(col("time_str")))

    //formattedDF.show()
    //formattedDF.printSchema()

    val currentDateDF = List(("2024-09-29", "15:30:00")).toDF("date_str", "time_str")

    currentDateDF.withColumn("add_50_days", date_add(col("date_str"), 50)).show()

    currentDateDF.withColumn("subtract_50_days", date_sub(col("date_str"), 50)).show()

    currentDateDF.withColumn("add_4_months", add_months(col("date_str"), 4)).show()

  }
}
