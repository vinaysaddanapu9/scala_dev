package Assignment3

object CheckRange {
  def main(args: Array[String]): Unit  = {
    val num = 90
     if(num >= 100 && num <= 1000){
        if(num%2 == 0){
          println("Even Number")
          println("Divided by 3: " +(num/3))
        }else{
          println("Odd Number")
          println("Divided by 2: "+(num/2))
        }
     }else{
       println("Wrong Number")
     }
  }
}
