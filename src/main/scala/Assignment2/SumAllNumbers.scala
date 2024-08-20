package Assignment2

object SumAllNumbers {
  def main(args: Array[String]): Unit = {

    var sum = 0
    for(i <- 1 to 50){
      sum = sum+i
    }
    println("Sum of all numbers 1 to 50: "+ sum)

  }
}
