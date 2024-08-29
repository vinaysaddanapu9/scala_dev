package ArraysAssignment

object Average {
  def main(args: Array[String]): Unit = {

    var sum = 0
    val arr = Array(10,20,30,40,50)
    for(i <- arr.indices) {
        sum = sum + arr(i)
    }

    var average =  (sum/arr.length)
    println("Average of Elements: "+ average)
  }
}
