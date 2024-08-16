object Divisibility2Or3 {

  private def checkDivisibility2Or3(num: Int): Boolean = {
    (num%2 == 0 || num%3 == 0)
  }

  def main(args: Array[String]): Unit = {
  println("Divisibility by 2 or 3: "+ checkDivisibility2Or3(9))

  }
}
