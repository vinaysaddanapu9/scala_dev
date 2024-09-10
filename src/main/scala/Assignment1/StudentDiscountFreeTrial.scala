package Assignment1

object StudentDiscountFreeTrial {

  def checkStudentDiscountOrFreeTrial(age: Int, freeTrial: Boolean): String = {
    if(age < 25 && freeTrial){
      "Student Discount"
    }else{
      "Free Trial"
    }
  }

  def main(args: Array[String]): Unit = {
    println(checkStudentDiscountOrFreeTrial(25, true))
  }
}
