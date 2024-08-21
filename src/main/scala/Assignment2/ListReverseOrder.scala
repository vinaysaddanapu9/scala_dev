package Assignment2

object ListReverseOrder {

  def main(args: Array[String]): Unit = {
    val numList = List(1,2,3,4,5)
    for(i <- numList.length -1 to 0 by -1){
      println(numList(i))
    }
  }

}
