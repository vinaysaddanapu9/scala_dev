package ArraysAssignment

object FindMaximumElement {

  def main(args: Array[String]): Unit = {
    val arr = Array(10.0,20.0,30.0,40.0,50.9)
    var max = arr(0)

    for(i <- arr.indices) {
          if(arr(i) > max){
             max = arr(i)
          }
    }
    println("Maximum Element in an array is: "+ max)
  }
}
