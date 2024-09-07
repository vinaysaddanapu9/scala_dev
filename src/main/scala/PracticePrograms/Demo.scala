package PracticePrograms

class Demo {

  def sum(x:Int, y:Int): Int = {
    var c = x + y
    c
  }

  def helloWorld(): Unit = {
    println("Hello World")
  }
}

object DemoObj {

  def main(args: Array[String]): Unit = {
    var demo = new Demo()
    demo.helloWorld()
    println("Sum: "+demo.sum(10,50))

  }
}
