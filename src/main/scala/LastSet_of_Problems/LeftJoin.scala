package LastSet_of_Problems

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, coalesce, col, lit, when}

object LeftJoin {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LeftJoin")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val employees = List(
      (1, "John", "Sales"),
      (2, "Jane", "HR"),
      (3, "Mark", "Finance"),
      (4, "Emily", "HR")
    ).toDF("employee_id","employee_name","department")

    val salaries = List(
      (1, 5000.00, "2024-01-10"),
      (2, 6000.00, "2024-01-15"),
      (4, 7000.00, "2024-01-20")
    ).toDF("employee_id","salary","credited_date")

    //Perform a LEFT JOIN on two DataFrames (employees and salaries), group by department, and
    //calculate average salary. Apply WHEN condition to determine salary categories and handle null
    //values with COALESCE.

    val condition = employees("employee_id") === salaries("employee_id")

    val joinType = "leftouter"

    val joined_df = employees.join(salaries, condition, joinType)

    val filledNaDF = joined_df.withColumn("salary", coalesce(col("salary"), lit("000.00")))
      .withColumn("credited_date", coalesce(col("credited_date"), lit("0000-00-00")))
      .withColumn("employee_id", coalesce(salaries("employee_id"), lit("0")))


    val aggregated_df = filledNaDF.groupBy(col("department")).agg(avg(col("salary")).as("avg_salary"))

    aggregated_df.withColumn("category", when(col("avg_salary") > 5000, "High")
      .when(col("avg_salary") > 4000, "Medium").otherwise("Low")).show()

  }
}
