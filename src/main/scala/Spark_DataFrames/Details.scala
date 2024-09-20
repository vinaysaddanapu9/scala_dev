package Spark_DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column}

object Details {

  def main(args: Array[String]) : Unit = {

    val spark = SparkSession.builder()
                .appName("SparK_Details_CSV")
                .master("local[*]")
                .getOrCreate()

    val df = spark.read.format("csv")
             .option("header", true)
             .option("path", "E:/Files/details.csv").load()

     df.select(col("id"), column("Salary")).show()
    //df.show()

  }
}
