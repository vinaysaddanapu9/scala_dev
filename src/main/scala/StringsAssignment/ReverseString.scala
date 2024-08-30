package StringsAssignment

object ReverseString {

  def main(args: Array[String]): Unit = {
    val str = "Practice"
    var reversedString = ""

    for(i <- str.length-1 to 0 by -1){
      reversedString = reversedString + str(i)
    }
    println("Actual String: "+ str)
    println("Reversed String: "+ reversedString)
  }
}
