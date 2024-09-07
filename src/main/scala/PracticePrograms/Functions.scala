package PracticePrograms

object Functions {

  object Math {
    def add(x:Int, y:Int): Int = {
      x+y
    }

    def square(x: Int) = x*x

    def sum(x: Int = 30, y:Int = 30): Int = {
      return x+y
    }

    def +(x: Int, y:Int): Int = {
      x+y
    }

    def **(x:Int): Int = x*x
  }

  def print(x: Int, y: Int): Unit = {
    println(x+y)
  }

  def add(x: Int, y: Int): Int = {
    return x+y;
  }

  def subtract(x: Int, y: Int): Int = {
    x-y;
  }

  def multiply(x: Int, y: Int): Int = x*y

  def divide(x: Int, y: Int): Int = x/y

  def helloWorld() = "Hello World"

  def main(args: Array[String]): Unit = {

    var add = (x:Int, y:Int) => x + y
    println(add(10,80))

    //println(Math.add(40,40))
    //println(Math square 3)
    //println("Add:"+add(5,5))
    //println("Subtract:"+ subtract(5,3))
    //println("Multiply:"+ multiply(2,3))
    //println("Divide: "+ divide(10,2))
    //println(helloWorld())

   // println(Math.sum(10))
   //print(100, 400)
    //println(Math.+(20,20))
    //println(Math ** 10)

  }
}
