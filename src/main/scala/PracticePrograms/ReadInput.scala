package PracticePrograms

object ReadInput {

  def main(args: Array[String]): Unit = {

    println("Enter the size")
    val size = scala.io.StdIn.readInt()
    var arr = new Array[Int](size)

    for(i <- 0 until arr.length){
        println("Enter the element at "+i)
        arr(i) = scala.io.StdIn.readInt()

    }
    println("Display the elements")
    for(i <- 0 until arr.length){
      println(arr(i))

    }
  }





}
