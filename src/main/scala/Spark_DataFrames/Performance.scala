package Spark_DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, when}

object Performance {

  def main(args: Array[String]) : Unit = {

    val spark = SparkSession.builder()
      .appName("SparK_Performance")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = List((1,"2024-01-10",8,"Good performance"),(2,"2024-01-15",9,"Excellent work!"),
    (3,"2024-02-20", 6,"Needs improvement"),(4,"2024-02-25", 7, "Good effort")
      ,(5,"2024-03-05",10,"Outstanding!"),(6,"2024-03-12", 5, "Needs improvement"))
      .toDF("employee_id","review_date","performance_score","review_text")

    //Add a new column performance_category
    val performanceCategoryDF = data.withColumn("performance_category", when(col("performance_score") >= 9, "Excellent")
      .when((col("performance_score") >= 7) && (col("performance_score") < 9), "Good").otherwise("Needs Improvement"))

    //performanceCategoryDF.show()

    //Filter the review_text with excellent
     val filteredDF = performanceCategoryDF.filter(col("review_text").contains("Excellent"))

    data.createOrReplaceTempView("reviews")

    //Spark SQL
    spark.sql("""
          SELECT *,
          CASE
          WHEN performance_score >= 9 THEN 'Excellent'
          WHEN performance_score >= 7 and performance_score < 9 THEN 'Good'
          ELSE 'Needs Improvement' END as performance_category
          FROM reviews
           """).show()

    //Average performance per month
    filteredDF.agg(avg(col("performance_score"))).show()

    //data.show()
  }
}
