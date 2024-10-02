package Lead_Lag_Assignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object LeadProblem2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LeadProblem2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val salesData = Seq(
      ("Product1", "Category1", 100),
      ("Product2", "Category2", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category3", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category3", 180)
    ).toDF("Product", "Category", "Revenue")

    //salesData.show()

    val window = Window.orderBy("Revenue")

    println("=========== LEAD ============")
    salesData.select(col("*"), lead(col("Revenue"), 1).over(window).as("NextValue")).show() //LEAD

    salesData.select(col("*"), lead(col("Revenue"), 2).over(window).as("Next_2_Value")).show() // LEAD NEXT2VALUE

    println("============== LAG ==============")
    salesData.select(col("*"), lag(col("Revenue"), 1).over(window).as("PreviousValue")).show() //LAG

    salesData.select(col("*"), lag(col("Revenue"), 2).over(window).as("Previous2Value")).show() //LAG PREVIOUS2VALUE


  }
}
