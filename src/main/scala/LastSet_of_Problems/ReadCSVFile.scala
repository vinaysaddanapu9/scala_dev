package LastSet_of_Problems

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, last, sum, when}

object ReadCSVFile {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("ReadCSVFile")
      .master("local[*]")
      .getOrCreate()

    //Read product data from a CSV file, calculate the sales per region using window aggregation, and
    //handle missing dates with the latest available data. Write the result in ORC format

    val products = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("path","E://Files/products.csv")
      .load()

    val filled_products = products.na.fill(Map("sales" -> 0))

    val window = Window.partitionBy("region")

    val aggregated_df = filled_products.select(col("*"), sum(col("sales")).over(window).as("total_sales"))

    aggregated_df.write.format("ORC")
      .option("overwrite", true)
      .option("path", "E://Files/products_output/").save()

    val regions = spark.read.format("csv")
      .option("header", true)
      .option("path","E://Files/regions.csv")
      .load()

    val window2 = Window.orderBy("date")
    val filledRegions = regions.withColumn("date", when(col("date").isNull, last(col("date")).over(window2)))

    filledRegions.write.format("ORC")
      .option("mode","overwrite")
      .option("path", "E://Files/regions_output")
      .save()
  }
}
