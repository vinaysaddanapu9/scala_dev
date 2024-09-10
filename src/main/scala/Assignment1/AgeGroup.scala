package Assignment1

object AgeGroup {

  private def ageGroupClassification(age: Int): String = {
    if(age > 20){
      "Adult"
    }else if(age > 13 && age < 19){
      "Teenager"
    } else{
      "Child"
    }
  }

  def main(args: Array[String]): Unit = {
   println(ageGroupClassification(15))
  }
}
