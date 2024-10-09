package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object HospitalPatientVisits {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("HospitalPatientVisits")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val patients = Seq(
      ("P001", "Alice", 30),
      ("P002", "Bob", 40),
      ("P003", "Charlie", 25)
    ).toDF("patient_id", "patient_name", "age")

    val visits = Seq(
      ("V001", "P001", "2024-01-01", "Routine Checkup"),
      ("V002", "P002", "2024-01-05", "Consultation"),
      ("V003", "P001", "2024-01-10", "Follow-up"),
      ("V004", "P003", "2024-01-12", "Emergency"),
      ("V005", "P001", "2024-01-15", "Routine Checkup")
    ).toDF("visit_id", "patient_id", "visit_date", "visit_reason")

    //patients.show()
    //visits.show()

    //Convert date str to data datatype
    val modified_visits = visits.withColumn("visit_date", to_date(col("visit_date")))
    //modified_visits.printSchema()

    val condition = patients("patient_id") === modified_visits("patient_id")

    val joinType = "inner"

     val joined_df = patients.join(modified_visits, condition, joinType)
                             .drop(modified_visits("patient_id"))
    joined_df.show()

    println("=============== Spark SQL =================")
    patients.createOrReplaceTempView("patients")
    modified_visits.createOrReplaceTempView("visits")

    spark.sql(
      """
        SELECT *
        FROM patients p
        JOIN visits v
        ON p.patient_id = v.patient_id
        """).show()

  }
}
