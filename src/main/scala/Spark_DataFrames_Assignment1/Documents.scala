package Spark_DataFrames_Assignment1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Documents {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

   val spark = SparkSession.builder()
     .appName("DocumentsExample")
     .master("local[*]")
     .getOrCreate()

    import spark.implicits._

    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ).toDF("doc_id", "content")

    documents.withColumn("content_category", when(col("content").like("%fox%"), "Animal Related")
    .when(col("content").like("%Lorem%"), "Placeholder Text")
      .when(col("content").like("%Spark%"), "Tech Related")).show(truncate = false)

    documents.createOrReplaceTempView("documents")

    spark.sql(
      """
        SELECT *,
        CASE
        WHEN content like '%fox%' THEN 'Animal Related'
        WHEN content like '%Lorem%' THEN 'Placeholder Text'
        WHEN content like '%Spark%' THEN 'Tech Related'
        END AS content_category
        FROM documents
        """).show(truncate = false)

    //documents.show(truncate = false)
  }
}
