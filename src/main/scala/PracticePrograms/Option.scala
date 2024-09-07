package PracticePrograms

object Option {

  def main(args: Array[String]): Unit = {

    val list = List(1,2,3)
    val map = Map(1 -> "Tom", 2 -> "Max", 3 -> "John")

    println(list.find(_ > 4).getOrElse(0))
    println(map.getOrElse(3, "No Name Found"))

    val opt1 : Option[Int] = Some(5)
    val opt2 : Option[Int] = None

    println(opt1.isEmpty)
    println(opt2.isEmpty)
  }

}
