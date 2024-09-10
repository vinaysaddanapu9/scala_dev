package PracticePrograms

import java.util.Date

object PartiallyAppliedFunctions {

  private def log(date: Date, message: String) = {
    println(date +" "+ message)
  }

  def main(args: Array[String]): Unit = {

    val sum = (a: Int, b: Int, c: Int) => a+b+c
    val f = sum(50, _: Int, _: Int)

    //println("Partially Applied Function: "+ f(120, 10))

    val date = new Date()
    val newLog = log(date, _: String)
    newLog(": The message 1")
    newLog(": The message 2")
    newLog(": The message 3")
    newLog(": The message 4")

  }
}
