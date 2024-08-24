package Assignment3

object OddIndex {

  def main(args: Array[String]): Unit = {
    val arr = Array(10,20,30,40,50,60,70,80)

    for(i <- arr.indices){
       if(i%2 != 0) println(arr(i))

    }
  }

}
