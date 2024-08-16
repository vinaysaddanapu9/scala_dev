object OddOrPrimeNumber {

   private def checkOddorPrimeNumber(num: Int): Boolean = {
     (num%2 != 0 || isPrime(num))
   }

  private def isPrime(num: Int): Boolean = {
    for(i <- 2 to num) {
         if (num%i == 0) false
    }
    true
  }

  def main(args: Array[String]): Unit = {
   println("Is Odd Number or Prime Number: "+checkOddorPrimeNumber(11))

  }
}
