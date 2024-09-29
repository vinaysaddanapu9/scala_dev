package Spark_DataFrames_Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

object SalesDifference {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("SalesDifference")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val sales = List(
      ("ProductA", "CategoryX", "2023-09-01", 100.0),
      ("ProductA", "CategoryX", "2023-09-02", 120.0),
      ("ProductA", "CategoryX", "2023-09-03", 130.0),
      ("ProductB", "CategoryY", "2023-09-01", 200.0),
      ("ProductB", "CategoryY", "2023-09-02", 210.0)
    ).toDF("product","category","date","sales")

    //sales.show()

    //Calculate the difference in sales between today and yesterday for each product

    val window = Window.partitionBy("product").orderBy("date")

    sales.select(col("product"), col("category"), col("date"), col("sales"),
      (col("sales") - lag(col("sales"), 1).over(window)).as("sale_difference")
    ).show()


  }
}
