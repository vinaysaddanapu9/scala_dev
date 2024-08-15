object CheckOddNumber {
   private def checkOddNumberAndDivisibleThree(num: Int): Boolean = {
     (num%2 != 0 && num%3 != 0)
   }

   def main(args: Array[String]): Unit = {
     println("Is OddNumber and not divisible by 3: "+checkOddNumberAndDivisibleThree(27))

   }
}
