package Spark_DataFrames_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BookAuthorPublication {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("BookAuthorPublication")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val books = Seq(
      ("B001", "Book One", "A001"),
      ("B002", "Book Two", "A002"),
      ("B003", "Book Three", "A001"),
      ("B004", "Book Four", "A003")
    ).toDF("book_id", "book_title", "author_id")

    val authors = Seq(
      ("A001", "Author One"),
      ("A002", "Author Two"),
      ("A003", "Author Three")
    ).toDF("author_id", "author_name")

    //books.show()
    //authors.show()

    val condition = books("author_id") === authors("author_id")

    val joinType = "inner"

    val joined_df = books.join(authors, condition, joinType).drop(authors("author_id"))
      joined_df.show()

  }
}
