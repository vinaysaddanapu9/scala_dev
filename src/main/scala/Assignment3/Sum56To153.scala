package Assignment3

object Sum56To153 {
  def main(args: Array[String]): Unit = {
    var sum = 0
    for(i <- 56 to 153) {
       sum = sum + i
    }
    println("Sum of all numbers from 56 to 153 is: "+ sum)
  }
}
