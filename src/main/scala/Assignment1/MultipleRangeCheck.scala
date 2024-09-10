package Assignment1

object MultipleRangeCheck {

  private def multipleRangeCheck(num: Int): String = {
    if(num>=1 && num <=10){
      "Number is in range between 1 and 10"
    } else if(num >= 20 && num <= 30){
        "Number is in range between 20 and 30"
    }else{
      "Out of defined range"
    }
  }

  def main(args: Array[String]): Unit = {
    println(multipleRangeCheck(25))
  }
}
