package ArraysAssignment

object SecondSmallest {
  def main(args: Array[String]): Unit = {

    val arr = Array(1, 8, 4, 5, 6, 7, 3, 9, 10)
    var smallest = arr(0)
    var largest = arr(1)
    var sec_smallest = 0

    for(i <- arr.indices){
      if(arr(i) < smallest){
         smallest = arr(i)
      }

      for(j <- arr.indices){
        if(arr(j) != smallest && arr(j) < largest){
          sec_smallest = arr(j)
        }
      }
    }
    println("Second Smallest: "+ sec_smallest)

  }
}
