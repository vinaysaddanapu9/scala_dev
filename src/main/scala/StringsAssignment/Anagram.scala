package StringsAssignment

object Anagram {

  def isAnagram(str1 : String, str2 : String): Boolean = {

    var str01 = str1.toLowerCase
    var str02 = str2.toLowerCase

    if(str01.length == str02.length) {

      var charArray1 = str01.toCharArray
      var charArray2 = str02.toCharArray

      charArray1.sorted.sameElements(charArray2.sorted)
    } else{
       false
    }
  }

  def main(args: Array[String]): Unit = {

    val str1 = "listen"
    val str2 = "slient"

    val result = isAnagram(str1, str2)
    if(result){
      println("Both are Anagram")
    }else{
      println("Both are not Anagram")
    }
  }
}
