package Assignment3

object DivisibleBy11 {

  def main(args: Array[String]): Unit = {
    var sum = 0

    println("All Numbers which are divisible by 11: ")
     for(i <- 250 to 550) {
       if(i%11 == 0) println(i)
     }
  }

}
