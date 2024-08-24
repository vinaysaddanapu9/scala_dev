package Assignment3

object CountEvenNumbersInRange {
  var count = 0
  val start = 50
  var end = 70
  def main(args: Array[String]): Unit = {
     for(i <- start to end){
        if(i%2 == 0) count = count+1
     }
    println("Count of Even Numbers between the range is: "+ count)
  }
}
