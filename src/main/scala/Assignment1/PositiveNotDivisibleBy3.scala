package Assignment1

object PositiveNotDivisibleBy3 {

  private def checkPositiveAndNotDivisibleBy3(num: Int): Boolean = {
    (num > 0 && num%3 != 0)
  }

  def main(args: Array[String]): Unit = {
   println("Check Positive and Not Divisible by 3: "+ checkPositiveAndNotDivisibleBy3(7))
  }
}
