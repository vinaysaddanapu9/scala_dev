package Assignment1

object Divisibility4Or6 {
  def checkDivisibility4Or6(num: Int): Boolean = {
    (num%4 == 0 || num%6 == 0)
  }

  def main(args: Array[String]): Unit = {
    println("Check Divisibility by 4 or 6: "+checkDivisibility4Or6(24))
  }
}
