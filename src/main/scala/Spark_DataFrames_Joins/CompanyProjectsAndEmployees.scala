package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CompanyProjectsAndEmployees {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("CompanyProjectsAndEmployees")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val projects = Seq(
      ("PR001", "Project Alpha"),
      ("PR002", "Project Beta"),
      ("PR003", "Project Gamma")
    ).toDF("project_id", "project_name")

    val employees = Seq(
      ("E001", "Alice"),
      ("E002", "Bob"),
      ("E003", "Charlie"),
      ("E004", "David")
    ).toDF("employee_id", "employee_name")

    val assignments = Seq(
      ("A001", "PR001", "E001"),
      ("A002", "PR001", "E002"),
      ("A003", "PR002", "E001"),
      ("A004", "PR003", "E003"),
      ("A005", "PR003", "E004")
    ).toDF("assignment_id", "project_id", "employee_id")

    //projects.show()
    //employees.show()
    //assignments.show()

    val condition = assignments("project_id") === projects("project_id")

    val condition2 = assignments("employee_id") === employees("employee_id")

    val joinType = "inner"

     val joined_df = assignments.join(projects, condition, joinType)
                    .join(employees, condition2, joinType)
                    .drop(projects("project_id"))
                    .drop(employees("employee_id"))

    joined_df.show()

    println("=============== Spark SQL ==================")
    employees.createOrReplaceTempView("employees")
    projects.createOrReplaceTempView("projects")
    assignments.createOrReplaceTempView("assignments")

    spark.sql(
      """
        SELECT *
        FROM assignments a
        JOIN projects p ON
        a.project_id = p.project_id
        JOIN employees e ON
        a.employee_id = e.employee_id
        """).show()

  }
}
