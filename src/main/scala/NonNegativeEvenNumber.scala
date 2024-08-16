object NonNegativeEvenNumber {
   private def checkNonNegativeEvenNumber(num: Int): Boolean = {
      num%2 == 0 || num > 0
   }

   def main(args: Array[String]): Unit = {
     println(checkNonNegativeEvenNumber(-8))
   }
}
