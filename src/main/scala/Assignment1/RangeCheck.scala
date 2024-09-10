package Assignment1

object RangeCheck {

  private def rangeCheck(num: Int): Boolean = {
    (num < -10 || num > 10)
  }

  def main(args: Array[String]): Unit = {
    println("Range Check between -10 and 10: "+ rangeCheck(-15))

  }

}
