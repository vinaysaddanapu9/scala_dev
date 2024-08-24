package Assignment3

object SimpleMath {

  def main(args: Array[String]): Unit = {
    val a = 50
    val b = 20
    val operator = "MUL"
    val result = operator match {
      case "ADD" => a+b
      case "SUB" => a-b
      case "MUL" => a*b
      case "DIV" => a/b
      case _ => None
    }
    println("Result of "+operator+" is: "+result)
  }

}
