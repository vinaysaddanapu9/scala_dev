package ArraysAssignment

object MergeArray {
  def main(args: Array[String]): Unit = {

    val arr1 = Array(1,2,3,4,5)
    val arr2 = Array(6,7,8,9,10)

    val newArr = Array.concat(arr1, arr2)
    newArr.foreach(println)
  }
}
