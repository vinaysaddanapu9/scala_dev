package WindowAggregationProblem

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, sum}

object RunningTotal {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("RunningTotal")
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

    //Calculate the running total of revenue for each product
    val window = Window.partitionBy("Category").orderBy("Product")

    val runningTotalDF = salesData.withColumn("RunningTotal", sum("Revenue").over(window))
      runningTotalDF.show()

    //Calculate the running avg of revenue for each product
     val runningAvgDF = salesData.withColumn("RunningAverage", avg("Revenue").over(window))
     runningAvgDF.show()


  }
}
