package ArraysAssignment

object SumAllElements {

  def main(args: Array[String]): Unit = {
    val arr = Array(1,2,3,4,5)
    var sum = 0
    
    for(i <- arr.indices){
       sum = sum + arr(i)
    }
    println("Sum of all elements: " +sum)
  }

}
