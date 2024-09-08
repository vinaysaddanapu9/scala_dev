package PracticePrograms

object ReadDouble {

  def main(args: Array[String]): Unit = {

    println("Enter the array size")
    val size = scala.io.StdIn.readInt()

    var arr = new Array[Double](size)

    for(i <- 0 until arr.length){
      println("Enter the element at "+i)
      arr(i) = scala.io.StdIn.readDouble()
    }

    println("Display the elements")
    for(i <- 0 until arr.length){
      println(arr(i))
    }

  }

}
