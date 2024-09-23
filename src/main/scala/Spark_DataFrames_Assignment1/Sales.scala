package Spark_DataFrames_Assignment1

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{col, when}

object Sales {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("SalesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sales = List(
      (1, 100),
      (2, 1500),
      (3, 300)
    ).toDF("sale_id", "amount")

    sales.withColumn("discount", when(col("amount") < 200, 0)
      .when((col("amount") > 200) && (col("amount") <= 1000), 10).when(col("amount") > 1000, 20)).show()

    sales.createOrReplaceTempView("sales")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN amount < 200 THEN 0
        WHEN amount BETWEEN 200 and 1000 THEN 10
        WHEN amount > 1000 THEN 20
        END as discount
        FROM sales
        """).show()

    //sales.show()

  }
}
