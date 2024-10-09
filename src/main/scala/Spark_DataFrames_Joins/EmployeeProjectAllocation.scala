package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object EmployeeProjectAllocation {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("EmployeeProjectAllocation")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val employees = Seq(
      ("E001", "Alice", "D001"),
      ("E002", "Bob", "D002"),
      ("E003", "Charlie", "D001"),
      ("E004", "David", "D003"),
      ("E005", "Eve", "D002")
    ).toDF("employee_id", "employee_name", "department_id")

    val projects = Seq(
      ("PR001", "Project Alpha"),
      ("PR002", "Project Beta"),
      ("PR003", "Project Gamma")
    ).toDF("project_id", "project_name")

    val allocations = Seq(
      ("A001", "E001", "PR001"),
      ("A002", "E002", "PR002"),
      ("A003", "E001", "PR003"),
      ("A004", "E003", "PR001"),
      ("A005", "E004", "PR003"),
      ("A006", "E005", "PR002")
    ).toDF("allocation_id", "employee_id", "project_id")

    val departments = Seq(
      ("D001", "HR"),
      ("D002", "Finance"),
      ("D003", "IT")
    ).toDF("department_id", "department_name")

    val condition = allocations("employee_id") === employees("employee_id")

    val condition2 = allocations("project_id") === projects("project_id")

    val condition3 = employees("department_id") === departments("department_id")

    val joinType = "inner"

    val joined_df = allocations.join(employees, condition, joinType)
      .join(projects, condition2, joinType)
      .join(departments, condition3, joinType)
      .drop(employees("employee_id"))
      .drop(employees("department_id"))

    joined_df.show()

    println("=============== Spark SQL =====================")
    employees.createOrReplaceTempView("employees")
    projects.createOrReplaceTempView("projects")
    allocations.createOrReplaceTempView("allocations")
    departments.createOrReplaceTempView("departments")

    spark.sql(
      """
        SELECT *
        FROM
        allocations a
        JOIN employees e ON
        a.employee_id = e.employee_id
        JOIN projects p ON
        a.project_id = p.project_id
        JOIN departments d ON
        e.department_id = d.department_id
        """).show()

  }
}
