package Pattern

object SquarePattern {
  def main(args: Array[String]): Unit = {

    for(i <- 1 to 4){
      for(j <- 1 to 4) {
        print("* ")
      }
      println("")
    }
  }

}
