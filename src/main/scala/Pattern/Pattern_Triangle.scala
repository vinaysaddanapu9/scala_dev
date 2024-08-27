package Pattern

object Pattern_Triangle {
  def main(args: Array[String]): Unit = {

    for(i <- 1 to 5) {
      for(j <- 1 to i) {
        if(i > 1 && j < i) {
          print("*_")
        }else{
          print("*")
        }
      }
      println("")
    }

  }
}
