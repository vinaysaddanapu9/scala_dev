package ArraysAssignment

object IntegerArray {
  def main(args: Array[String]): Unit = {

    val arr = Array(1,2,3,4,5)
    for(i <- arr.indices) {
         println(arr(i))
    }

  }
}
