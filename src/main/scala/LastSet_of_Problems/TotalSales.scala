package LastSet_of_Problems

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, when}

object TotalSales {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("TotalSales")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val customers = List(
      (1, "Alice", "2023-01-15"),
      (2, "Bob", "2023-02-10"),
      (3, "Charlie", null)
    ).toDF("customer_id", "customer_name", "registration_date")

     val orders = List(
       (1,"electronics",300.50,"2023-01-20"),
       (1,"clothing",10.00,"2023-01-25"),
       (2,"groceries",120.00,"2023-02-15"),
       (3,"clothing",50.00,"2023-02-20")
    ).toDF("customer_id","order_type","sales_amount","order_date")

    orders.show()

     val new_customers = customers.na.fill(Map("registration_date" -> "0000-00-00"))
     new_customers.show()

    //Calculate the total sales per customer for different order types and apply WHEN to categorize
    //customers based on sales amount (High/Medium/Low). Handle nulls for missing sales and join
    //with customer info.
    val condition = orders("customer_id") === new_customers("customer_id")
    val joinType = "inner"

    val joined_df = orders.join(new_customers, condition, joinType)
    joined_df.show()

    val aggregated_df = joined_df.groupBy(customers("customer_id"))
      .agg(sum(col("sales_amount")).as("total_sales_amount"))

    aggregated_df.withColumn("category", when(col("total_sales_amount") > 200, "High")
      .when(col("total_sales_amount") > 100, "Medium").otherwise("Low")).show()


  }

}
