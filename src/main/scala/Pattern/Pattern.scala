package Pattern

object Pattern {

  def main(args: Array[String]): Unit = {
    var num = 1
    for(i <- 1 to 5){
      for(j <- 1 to i){
        print(i+" ")
      }
      num = num+1
      println("")
    }

  }

}
