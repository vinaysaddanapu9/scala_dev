package Assignment2

object StringReverse {

  def main(args: Array[String]): Unit = {
    var str = "Scala"
    var reversedString=""
    for(i <- str.length-1  to 0 by -1){
      reversedString += str(i)
    }
    println("Reverse String is: "+reversedString)
  }

}
