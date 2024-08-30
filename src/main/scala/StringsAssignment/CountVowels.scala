package StringsAssignment

object CountVowels {

  def main(args: Array[String]): Unit = {
    val word = "Plough"
    var count = 0

    for(i <- word){
       if(i == 'a' || i == 'e' || i == 'i' || i == 'o' || i == 'u'){
          count = count+1
       }
    }

    println("Count of vowels: "+ count)
  }
}
