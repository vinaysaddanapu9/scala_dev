package Assignment2

object VowelsCount {
  def main(args: Array[String]): Unit = {
    val vowelsList = List('a','e','i','o','u')
    val str = "ramesh"
    val charArray = str.toCharArray
    var vowelsCount = 0
    for(char <- charArray){
         if(vowelsList.contains(char)) vowelsCount += 1
    }
    println(s"VowelsCount for $str is: $vowelsCount")
  }

}
