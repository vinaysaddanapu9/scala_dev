package Assignment1

object NonPositiveEvenNumber {

  private def checkNonPositiveAndEven(num: Int): Boolean = {
     (num < 0 && num%2 == 0)
  }

  def main(args: Array[String]): Unit = {
     println("Non Positive and Even Number: "+checkNonPositiveAndEven(-6))
  }
}
