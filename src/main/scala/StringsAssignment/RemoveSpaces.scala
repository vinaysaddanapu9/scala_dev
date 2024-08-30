package StringsAssignment

object RemoveSpaces {

  def main(args: Array[String]): Unit = {
    var originalString = "Data Engineer is a  good job"
    var stringWithoutSpaces = originalString.replaceAll(" ", "")
    println("String without spaces: "+ stringWithoutSpaces)
  }
}
