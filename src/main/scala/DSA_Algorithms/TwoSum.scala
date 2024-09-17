package DSA_Algorithms

//Most asked interview question
object TwoSum {

  def main(args: Array[String]): Unit = {

    var arr=Array(10,20,30,40,50,60,70,80)

    var targetsum=100

    for(i<-0 until arr.length){

      for(j<-i+1 until arr.length){
        if(arr(i)+arr(j)==targetsum){
          print(arr(i),arr(j))
        }

      }
      println()
    }
  }
}
