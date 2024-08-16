object Divisibility3And5 {

  private def checkDivisibility3And5(num: Int): Boolean = {
     (num%3 == 0 && num%5 == 0)
  }

  def main(args: Array[String]): Unit = {
    println("Divisibility by 3 and 5: " +checkDivisibility3And5(15))
  }

}
