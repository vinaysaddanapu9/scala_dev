package Spark_DataFrames_Assignment3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, year}

object HospitalPatientVisitAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("HospitalPatientVisitAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val patientVisits = List(
        ("P001", "2024-03-01", "John Doe", "Heart Checkup", "Cardiology"),
        ("P002", "2024-03-02", "Jane Smith", "Dental Checkup", "Dentistry"),
        ("P003", "2024-03-01", "Mike Johnson", "Skin Rash", "Dermatology"),
        ("P004", "2024-03-03", "Emily Davis", "Regular Checkup", "General Medicine"),
        ("P005", "2024-03-02", "Chris Brown", "Eye Exam", "Ophthalmology"),
        ("P006", "2024-03-01", "Anna Wilson", "Blood Test", "Laboratory"),
        ("P007", "2024-03-04", "David Lee", "Allergy Test", "Allergy"),
        ("P008", "2024-03-05", "Sarah Miller", "Heart Checkup", "Cardiology"),
        ("P009", "2024-03-02", "Jason Taylor", "Regular Checkup", "General Medicine"),
        ("P010", "2024-03-03", "Olivia Moore", "Skin Checkup", "Dermatology")
    ).toDF("patient_id","visit_date","doctor_name","check_up","department")

    patientVisits.show()

    patientVisits.withColumn("visit_year", year(col("visit_date"))).show()

    patientVisits.filter(col("department").startsWith("General")
      && col("check_up") === "Regular Checkup").show()

  }
}
