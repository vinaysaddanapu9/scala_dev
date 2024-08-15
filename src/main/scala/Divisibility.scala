object Divisibility {

  private def checkDivisible(num: Int): Boolean = {
    (num%4 == 0 || num%6 == 0)
  }

  def main(args: Array[String]): Unit = {
    println("Divisible by 4 or 6: "+ checkDivisible(18))
  }
}
