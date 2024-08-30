package ArraysAssignment

object ArraySorted {

  def main(args: Array[String]): Unit = {

    val unSortedArray = Array(2,3,5,7,8,9,10,1)
    val sortedArray = Array(1,2,3,4,5,6,7,8,9,10)
    var isOrdered = false
    for(i <- unSortedArray.indices) {
       if(i+1 == unSortedArray(i)){
         isOrdered = true
       }
    }
    println("Is the Array Sorted: "+ isOrdered)
  }

}
