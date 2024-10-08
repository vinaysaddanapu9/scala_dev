package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object EmployeeDepartmentJoin {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("EmployeeDepartmentJoin")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val employees = Seq(
      ("E001", "Alice", "D001"),
      ("E002", "Bob", "D002"),
      ("E003", "Charlie", "D001"),
      ("E004", "David", "D003"),
      ("E005", "Eve", "D002"),
      ("E006", "Frank", "D001"),
      ("E007", "Grace", "D004")
    ).toDF("employee_id", "employee_name", "department_id")

    //employees.show()

    val departments = Seq(
      ("D001", "HR"),
      ("D002", "Finance"),
      ("D003", "IT"),
      ("D004", "Marketing"),
      ("D005", "Sales")
    ).toDF("department_id", "department_name")

    //departments.show()

    val condition = employees("department_id") === departments("department_id")

    val joinType = "inner"

    val joined_df = employees.join(departments, condition, joinType)
    joined_df.show()

    println("================== Spark SQL =====================")
    employees.createOrReplaceTempView("employees")
    departments.createOrReplaceTempView("departments")

    spark.sql(
      """
        SELECT *
        FROM employees e
        JOIN departments d
        ON e.department_id = d.department_id
        """).show()

  }
}
