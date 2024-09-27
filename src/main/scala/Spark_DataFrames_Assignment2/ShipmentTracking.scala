package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object ShipmentTracking {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("ShipmentTracking")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val shipments = List(
        (1, "Asia", 15000, "2024-01-10"),
        (2, "Europe", 6000, "2024-01-15"),
        (3, "Asia", 3000, "2024-02-20"),
        (4, "Asia", 20000, "2024-02-25"),
        (5, "North America", 4000, "2024-03-05"),
        (6, "Asia", 8000, "2024-03-12")
    ).toDF("shipment_id", "destination", "shipment_value", "shipment_date")

    //shipments.show()
    val shipmentsCategory = shipments.withColumn("value_category", when(col("shipment_value") > 10000, "High")
      .when((col("shipment_value") >= 5000) && (col("shipment_value") <= 10000), "Medium")
    .when(col("shipment_value") < 5000, "Low"))

    shipmentsCategory.show()

    shipments.filter(col("destination").contains("Asia")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //shipment value for each value_category
    shipmentsCategory.groupBy(col("value_category")).agg(sum(col("shipment_value")).as("total_shipment_value"),
      avg(col("shipment_value")),min(col("shipment_value")), max(col("shipment_value"))).show()

    println("============== Spark SQL ==============")
    shipments.createOrReplaceTempView("shipments")
    shipmentsCategory.createOrReplaceTempView("shipments_category")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN shipment_value > 10000 THEN 'High'
        WHEN shipment_value BETWEEN 5000 and 10000 THEN 'Medium'
        WHEN shipment_value < 5000 THEN 'Low'
        END AS value_category
        FROM shipments
        """).show()

    spark.sql(
      """
        SELECT *
        FROM shipments
        WHERE destination like '%Asia%'
        """).show()

    spark.sql(
      """
        SELECT
        value_category,
        sum(shipment_value) as total_shipment_value, avg(shipment_value),
        min(shipment_value), max(shipment_value)
        FROM shipments_category
        GROUP BY value_category
        """).show()

  }

}
