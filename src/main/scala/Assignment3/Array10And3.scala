package Assignment3

object Array10And3 {

  def main(args: Array[String]): Unit = {
    val arr = Array(10,20,45,67,84,78)

    for(i <- arr){
       if(i%10 == 0 && i%3 == 0) println(i)
    }
  }

}
