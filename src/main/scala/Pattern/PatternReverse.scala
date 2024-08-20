package Pattern

object PatternReverse {

  def main(args: Array[String]): Unit = {
    var num = 15
    for(i <- 5 to 1 by -1) {
      for (j <- 1 to i) {
        print(num + " ")
        num = num - 1
      }
      println("")
    }
  }

}
