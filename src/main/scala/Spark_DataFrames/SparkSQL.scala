package Spark_DataFrames

import org.apache.spark.sql.SparkSession

object SparkSQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparK_SQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val data = List(("mohan",56,100),("vijay",45,23),("ajay",67,68)).toDF("name","age","marks")

    data.createOrReplaceTempView("customer")

    spark.sql("""
    SELECT name,
           age,
           marks,
           CASE
               WHEN marks > 50 THEN 'grade A'
               WHEN marks BETWEEN 30 AND 50 THEN 'grade B'
               ELSE 'fail'
           END AS Grade
    FROM customer
""").show()

  }
}
