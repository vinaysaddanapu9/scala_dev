package Spark_DataFrames_Assignment1

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Average {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Average")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val scoreData = Seq(
      ("Alice", "Math", 80),
      ("Bob", "Math", 90),
      ("Alice", "Science", 70),
      ("Bob", "Science", 85),
      ("Alice", "English", 75),
      ("Bob", "English", 95)
    ).toDF("Student", "Subject", "Score")

    //Find the average score for each subject and the maximum score for each student
    scoreData.groupBy("Subject").avg("Score").show()

    //Maximum Score for each student
    scoreData.groupBy("Student").max("Score").show()

    //scoreData.show()

  }
}
