object MemberShipOrDiscount {

  private def checkEligibilityForMemberShipOrDiscount(amount: Int): String = {
    if(amount > 200){
      "Eligible for Discount"
    }else {
      "Loyalty Card:"+ true
    }
  }

  def main(args: Array[String]): Unit = {
    println(checkEligibilityForMemberShipOrDiscount(210))
  }
}
