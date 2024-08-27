package Pattern

object InvertedTriangle {

  def main(args: Array[String]): Unit = {

    for(i <- 5 to  1 by -1){
      for(j <- 1 to i) {
         print("* ")
      }
      println("")
    }

  }
}
