package Spark_DataFrames_Assignment1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Login {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LoginExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val logins = List(
      (1, "09:00"),
      (2, "18:30"),
      (3, "14:00")
    ).toDF("login_id", "login_time")

    logins.withColumn("is_morning", when(col("login_time") < "12:00", true).otherwise(false)).show()

    logins.createOrReplaceTempView("logins")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN login_time < '12:00' THEN true
        ELSE false
        END as is_morning
        FROM logins
        """).show()

    //logins.show()

  }
}
