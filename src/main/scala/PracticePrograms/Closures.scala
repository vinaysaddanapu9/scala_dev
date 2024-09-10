package PracticePrograms

object Closures {

  private var number = 10
  val add = (x: Int) => {
    number = x+number
    number
  }

  def main(args: Array[String]): Unit = {
     number = 100
     println(add(90))
  }
}
