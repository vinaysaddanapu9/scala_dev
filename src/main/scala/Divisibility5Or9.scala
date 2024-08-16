object Divisibility5Or9 {

  private def checkDivisibility5Or9(num: Int): Boolean = {
     (num%5 == 0 || num%9 == 0)
  }

  def main(args: Array[String]): Unit = {
    println("Divisibility by 5 or 9: "+checkDivisibility5Or9(45))
  }
}
