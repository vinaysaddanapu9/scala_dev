package Assignment2

object SumAllEvenNumbers {

  def main(args: Array[String]): Unit = {
    var num = 2
    var sum = 0
    while(num <= 20){
      if(num%2 == 0) sum = sum + num
      num = num + 1
    }
    println("Sum of All Even Numbers from 1 to 20: "+ sum)
  }

}
