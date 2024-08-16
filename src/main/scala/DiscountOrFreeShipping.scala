object DiscountOrFreeShipping {

  def checkEligibilityDiscountOrFreeShipping(amount: Int): String = {
    if(amount > 150){
      "Eligible for Discount"
    }else if(amount > 100){
      "Qualifies for free shipping"
    }else{
      "Not Eligible"
    }
  }

  def main(args: Array[String]): Unit = {
    println(checkEligibilityDiscountOrFreeShipping(120))
  }
}
