package Assignment3

object BiggestNumber {
  def main(args: Array[String]): Unit = {
    val a = 20
    val b = 10
    val c = 30

    if(a > b && a > c){
      println("A is bigger number")
    }else if(b > a && b > c){
      println("B is bigger number")
    }else{
      println("C is bigger number")
    }

  }
}
