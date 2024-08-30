package ArraysAssignment

object ArraySearch {
  def main(args: Array[String]): Unit = {

    val words = Array("apple","microsoft","facebook","twitter")
    val searchWord = "apple"
    var result = ""
    for(i <- words.indices){
       if(searchWord == words(i)) {
            result = "Word found: "+ words(i)
         }
       }
       if(result.isEmpty) println("No matching word found") else println(result)
  }
}
