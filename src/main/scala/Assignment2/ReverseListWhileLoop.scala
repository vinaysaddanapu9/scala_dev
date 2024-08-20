package Assignment2

object ReverseListWhileLoop {

  def main(args: Array[String]): Unit = {
    var list = List(1,2,3,4,5)
     var num = list.length-1
     while(num >= 0) {
       println(list(num))
       num = num - 1
     }
  }

}
