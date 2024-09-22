package Spark_DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Products {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
       .appName("Products")
       .master("local[*]")
       .getOrCreate()

    import spark.implicits._

    val products = List((1, 30.5), (2, 150.75),
    (3, 75.25)).toDF("product_id", "price")

    products.withColumn("price_range", when(col("price") < 50, "Cheap")
      .when((col("price") > 50) && (col("price") <= 100), "Moderate").otherwise("Expensive")).show()

    //products.show()
  }
}
