object Divisibility3Or8 {

   private def checkDivisiblity3or8(num: Int): Boolean = {
     (num%3 == 0 || num%8 == 0)
   }

  def main(args: Array[String]): Unit = {
   println("Check Divisibility by 3 or 8: "+ checkDivisiblity3or8(24))
  }
}
