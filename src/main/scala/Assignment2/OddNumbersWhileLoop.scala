package Assignment2

object OddNumbersWhileLoop {

  def main(args: Array[String]): Unit = {

    var num = 1
    while(num <= 15){
      if(num%2 != 0) println(num)
      num = num + 1
    }

  }
}
