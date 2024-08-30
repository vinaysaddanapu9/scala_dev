package StringsAssignment
import scala.collection.mutable.Map

object CountOccurences {

  def main(args: Array[String]): Unit = {

    var ocurrenceMap = Map[Char, Int]()
    val str = "dadddy"

    for(i <- str){
        var count = 0
       for(j <- str){
           if(i == j) count = count + 1
       }
      ocurrenceMap.put(i, count)
    }
    println("Count of occurences:")
    ocurrenceMap.foreach(println)
  }
}
