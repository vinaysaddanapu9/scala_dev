object NonNegativeNotDivisibleBy7 {

  private def checkNonNegativeNotDivisibleBy7(num: Int): Boolean = {
      (num > 0 && num%7 != 0)
  }

  def main(args: Array[String]): Unit = {
    print("Non Negative and Not Divisible By 7: "+ checkNonNegativeNotDivisibleBy7(14))
  }

}
