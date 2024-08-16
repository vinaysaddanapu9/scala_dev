object Divisibility2Or5 {

  def checkDivisibility2or5(num: Int): Boolean = {
     (num%2 == 0 || num%5 == 0)
  }

  def main(args: Array[String]): Unit = {
    println("Divisibility by 2 or 5: "+ checkDivisibility2or5(25))
  }
}
