object MultipleOf3And7 {

  private def checkMultiple(num: Int): Boolean = {
     (num%3 == 0 && num%7 == 0)
  }

  def main(args: Array[String]): Unit = {
    println("Multiple of 3 and 7: "+ checkMultiple(21))
  }
}
