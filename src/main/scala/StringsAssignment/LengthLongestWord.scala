package StringsAssignment

object LengthLongestWord {
  def main(args: Array[String]): Unit = {
    val str = "My Laptop is good condition"

    val strArray = str.split(" ")
    var lg_word_count = strArray(0).length
    var res = ""

    for(word <- strArray) {
        if(word.length > lg_word_count){
          lg_word_count = word.length
          res = word +" -> "+ lg_word_count
        }
    }
   println("Longest word & Count is: "+ res)
  }
}
