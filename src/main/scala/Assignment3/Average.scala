package Assignment3

object Average {
  def main(args: Array[String]): Unit = {
    var sum = 0
    var count = 0

    for(i <- 24 to 100){
      sum = sum + i
      count = count + 1
    }
    val average = sum/count
    println("Average of the numbers: "+ average)
  }

}
