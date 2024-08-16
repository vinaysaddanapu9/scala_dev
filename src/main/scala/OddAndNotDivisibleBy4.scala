object OddAndNotDivisibleBy4 {

  def checkOddAndNotDivisibleBy4(num: Int): Boolean = {
    (num%2 != 0 && num%4 != 0)
  }

  def main(args: Array[String]): Unit = {
    println("Odd Number and not divisible by 4: " +checkOddAndNotDivisibleBy4(15))
  }
}
