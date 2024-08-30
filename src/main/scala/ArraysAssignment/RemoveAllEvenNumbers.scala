package ArraysAssignment

object RemoveAllEvenNumbers {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,2,3,4,5,6,7,8,9,10)

    var oddArray = new Array[Int](10)
    var k = 0
    for(i <- arr.indices){
       if(arr(i)%2 != 0) {
         oddArray(k) = arr(i)
         k = k+1
       }
    }
    oddArray.foreach(println)

  }
}
