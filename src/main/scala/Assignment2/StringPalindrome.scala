package Assignment2

object StringPalindrome {

  def main(args: Array[String]): Unit = {
    var str = "dad"
    var reverseString=""
    for(i <- str.length-1 to 0 by -1){
        reverseString += str(i)
    }
    if(reverseString == str){
      println("It's a palindrome")
    }else{
      println("It's not a palindrome")
    }
  }

}
