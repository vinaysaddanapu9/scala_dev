package Assignment2

object Factorial {
  def main(args: Array[String]): Unit = {
    var num = 3
     var result = 1
     for(i <- 1 to num){
       result = result * i
     }
     println("Factorial of "+num+" is: " + result)
  }
}
