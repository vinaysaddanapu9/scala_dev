package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PatientVisitDetails {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("PatientVisitDetails")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val patients = Seq(
      ("P001", "Alice"),
      ("P002", "Bob"),
      ("P003", "Charlie"),
      ("P004", "David")
    ).toDF("patient_id", "patient_name")

    val visits = Seq(
      ("V001", "P001", "D001", "H001"),
      ("V002", "P002", "D002", "H002"),
      ("V003", "P003", "D001", "H003"),
      ("V004", "P001", "D003", "H001"),
      ("V005", "P004", "D002", "H002")
    ).toDF("visit_id", "patient_id", "doctor_id", "hospital_id")

    val doctors = Seq(
      ("D001", "Dr. Smith"),
      ("D002", "Dr. Johnson"),
      ("D003", "Dr. Lee")
    ).toDF("doctor_id", "doctor_name")

    val hospitals = Seq(
      ("H001", "City Hospital"),
      ("H002", "County Hospital"),
      ("H003", "General Hospital")
    ).toDF("hospital_id", "hospital_name")

    val condition = visits("patient_id") === patients("patient_id")

    val condition2 = visits("doctor_id") === doctors("doctor_id")

    val condition3 = visits("hospital_id") === hospitals("hospital_id")

    val joinType = "inner"

    val joined_df = visits.join(patients, condition, joinType)
      .join(doctors, condition2, joinType)
      .join(hospitals, condition3, joinType)

    joined_df.show()

    println("================= Spark SQL ====================")
    patients.createOrReplaceTempView("patients")
    visits.createOrReplaceTempView("visits")
    doctors.createOrReplaceTempView("doctors")
    hospitals.createOrReplaceTempView("hospitals")

    spark.sql(
      """
        SELECT *
        FROM visits v
        JOIN patients p ON
        v.patient_id = p.patient_id
        JOIN doctors d ON
        v.doctor_id = d.doctor_id
        JOIN hospitals h ON
        v.hospital_id = h.hospital_id
        """).show()

  }

}
