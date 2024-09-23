package Spark_DataFrames_Assignment1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Inventory {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("InventoryExample")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._

    val inventory = List(
      (1, 5),
      (2, 15),
      (3, 25)
    ).toDF("item_id", "quantity")

    inventory.withColumn("stock_level", when(col("quantity") < 10, "Low")
      .when((col("quantity") > 10) && (col("quantity") <= 20), "Medium").otherwise("High")).show()

    inventory.createOrReplaceTempView("inventory")

    //Using Spark SQL
    spark.sql(
      """
        SELECT *,
        CASE
        WHEN quantity < 10 THEN 'Low'
        WHEN quantity BETWEEN 10 AND 20 THEN 'Medium'
        ELSE 'HIGH'
        END AS stock_level FROM inventory
        """).show()

    //inventory.show()
  }
}
