package StringsAssignment
import scala.collection.mutable.LinkedHashMap

object NonRepeatedCharacter {

  def main(args: Array[String]): Unit = {

    var occurencesMap = LinkedHashMap[Char, Int]()
    val str = "rummy"

    for(i <- str){
      var count = 0
      for(j <- str){
        if(i == j) count = count + 1
      }
      if(count < 2) occurencesMap.put(i, count)
    }

    val (key, value) = occurencesMap.head
    println("First Non Repated Character in string: "+ key)

  }
}
