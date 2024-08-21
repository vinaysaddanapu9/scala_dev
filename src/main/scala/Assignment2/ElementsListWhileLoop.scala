package Assignment2

object ElementsListWhileLoop {

  def main(args: Array[String]): Unit = {
    val numList = List(1,2,3,4,5)
    val len = numList.length - 1
    var num = 0
    while(num <= len) {
      println(numList(num))
      num = num + 1
    }

  }
}
