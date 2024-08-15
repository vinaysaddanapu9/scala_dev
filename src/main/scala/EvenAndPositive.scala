object EvenAndPositive {

  private def isEvenAndPositive(num: Int): Boolean = {
    val result = (num > 0 && num%2 == 0)
    result
  }

  def main(args: Array[String]): Unit= {
   println("Is Number Even and Positive: "+ isEvenAndPositive(14))
  }

}
