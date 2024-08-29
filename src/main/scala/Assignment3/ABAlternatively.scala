package Assignment3

object ABAlternatively {

  def main(args: Array[String]): Unit = {

    for(i <- 0 until 100){
      if(i%2 == 0){
        print("A")
      } else{
        print("B")
      }
      print(",")
    }
  }

}
