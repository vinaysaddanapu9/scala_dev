package Assignment1

object Divisiblity5And7 {
  private def checkDivisiblity5And7(num: Int): Boolean = {
     (num%5 == 0 && num%7 == 0)
  }

  def main(args: Array[String]): Unit = {
    println("Check Divisibility by 5 and 7: "+ checkDivisiblity5And7(35))
  }
}
