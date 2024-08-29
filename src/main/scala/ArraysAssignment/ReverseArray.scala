package ArraysAssignment

object ReverseArray {

  def main(args: Array[String]): Unit = {
    val strArr = Array("apple", "mango", "banana", "kiwi")

    println("Reverse of array elements: ")

    for(element <- strArr.reverse){
      println(element)
    }
  }

}
