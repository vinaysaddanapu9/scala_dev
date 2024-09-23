import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession, functions => F}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

object First {

  def main(args: Array[String]): Unit = {

    //   val spark=SparkSession.builder()
    //     .appName("karthik")
    //     .master("local[*]")
    //     .getOrCreate()

    val sparkconf=new SparkConf()
    sparkconf.set("spark.app.Name","karthik")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark=SparkSession.builder()
      .config( sparkconf)
      .getOrCreate()

    //  val df=spark.read.csv("C:/Users/Karthik Kondpak/Documents/details.csv")

    //    val df=spark.read
    //      .format("csv")
    //      .option("header",true)
    //      .option("path","C:/Users/Karthik Kondpak/Documents/details.csv")
    //      .load()

    //   df.show(2,false)

    //   df.select(col("id"),column("Salary")).show()

    import spark.implicits._

    val data=List(("mohan",56,100),("vijay",45,23),("ajay",67,68)).toDF("name","age","marks")

    //    data.select(
    //       col("name")
    //        ,col("age")
    //        ,col("marks"),
    //        when(col("age")>55 && col("name").endsWith("n"),"senior").otherwise("junior").alias("status")
    //      ).show()

    //    data.filter(col("age")>55 && col("name").startsWith("m")).show()

    data.withColumn("status",when(col("age")>55 && col("name").endsWith("n"),"senior").otherwise("junior")).show()

    scala.io.StdIn.readLine()
    }

}
