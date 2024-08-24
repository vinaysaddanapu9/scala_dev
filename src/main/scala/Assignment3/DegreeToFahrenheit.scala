package Assignment3

object DegreeToFahrenheit {
  def main(args: Array[String]): Unit = {
    val degrees = 80
    val fahrenheit = (degrees * 9/5) + 32
    println(s"$degrees C is equal to $fahrenheit F")
  }

}
