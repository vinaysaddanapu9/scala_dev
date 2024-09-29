package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format}

object DateFormats {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("DateFormats")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df = Seq(("2023-10-07", "15:30:00")).toDF("date", "time")
    //df.show()

    val formattedDF = df.withColumn("formatted_date", date_format(col("date"), "yyyy.MM.dd"))
      .withColumn("formatted_time", date_format(col("time"), "HH.mm.ss"))

    formattedDF.show()
    df.withColumn("formatted_date", date_format(col("date"), "yyyy")).show()
    df.withColumn("formatted_date", date_format(col("date"), "MM")).show()

  }
}
