package Assignment1

object SeniorStudentDiscount {

  private def checkSeniorOrStudentDiscount(age: Int): String = {
    if(age > 60){
        "Senior Citizen Discount"
    }else if(age < 25){
      "Student Discount"
    }else{
      "No Discount"
    }
  }

  def main(args: Array[String]): Unit = {
    println(checkSeniorOrStudentDiscount(63))
  }
}
