package Assignment2

object StarPattern {

  def main(args: Array[String]): Unit = {

    for(i <- 1 to 4) {
      for(j <- 1 to i){
        print("*")
      }
      println("")
    }

  }

}
