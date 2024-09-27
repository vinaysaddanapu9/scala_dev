package Spark_DataFrames_Assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum, when, min, max}

object EmployeeTrainingRecords {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
                .appName("EmployeeTrainingRecords")
                .master("local[*]")
                .getOrCreate()

    import spark.implicits._
    val trainingRecords = List(
        (1, 1, 50, "Tech"),
        (2, 2, 25, "Tech"),
        (3, 3, 15, "Management"),
        (4, 4, 35, "Tech"),
        (5, 5, 45, "Tech"),
        (6, 6, 30, "HR")
    ).toDF("record_id", "employee_id", "training_hours", "training_type")

    //trainingRecords.show()
    val trainingStatusDF = trainingRecords.withColumn("training_status", when(col("training_hours") > 40, "Extensive")
      .when((col("training_hours") >= 20) and (col("training_hours") <= 40), "Moderate")
      .when(col("training_hours") < 20, "Minimal"))

    trainingStatusDF.show()

    trainingRecords.filter(col("training_type").startsWith("Tech")).show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) training_hours
    //for each training_status
    trainingStatusDF.groupBy(col("training_status")).agg(sum(col("training_hours")), avg(col("training_hours")),
      min(col("training_hours")), max(col("training_hours"))).show()

    println("============ Spark SQL =================")
    trainingRecords.createOrReplaceTempView("training_records")
    trainingStatusDF.createOrReplaceTempView("training_status_tab")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN training_hours > 40 THEN 'Extensive'
        WHEN training_hours BETWEEN 20 and 40 THEN 'Moderate'
        WHEN training_hours < 20 THEN 'Minimal'
        END AS training_status
        FROM training_records
        """).show()

    spark.sql(
      """
        SELECT *
        FROM training_records
        WHERE training_type like 'Tech%'
        """).show()

    spark.sql(
      """
        SELECT
        training_status,
        sum(training_hours), avg(training_hours),
        min(training_hours), max(training_hours)
        FROM training_status_tab
        GROUP BY training_status
        """).show()

  }
}
