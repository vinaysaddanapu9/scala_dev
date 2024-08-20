package Assignment2

object EvenNumbersWhileLoop {

  def main(args: Array[String]): Unit = {

    var num = 2
    while(num <= 10){
       if(num%2 == 0) println(num)
       num = num + 1
    }
  }
}
