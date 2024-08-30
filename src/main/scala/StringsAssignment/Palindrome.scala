package StringsAssignment

object Palindrome {

  def main(args: Array[String]): Unit = {

    val str = "rotator"
    var reverseString = ""

     for(i <- str.length - 1 to 0 by -1){
       reverseString = reverseString + str(i)
     }

    if(str == reverseString){
      println("It's a palindrome")
    }else{
      println("It's not a palindrome")
    }
  }
}
