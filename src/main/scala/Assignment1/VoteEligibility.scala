package Assignment1

object VoteEligibility {
  def checkEligiblity(age: Int): String = {
    if(age >= 18) {
      "Eligible to vote"
    } else if(age >= 16) {
      "Eligible to drive"
    }else{
      "Not Eligible"
    }
  }

  def main(args: Array[String]): Unit = {
    println("Age 18: "+ checkEligiblity(20))
    println("Age 16: "+ checkEligiblity(16))

  }
}
