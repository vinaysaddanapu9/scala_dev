package PracticePrograms

object Palindrome {

  def main(args:Array[String]):Unit =  {

    var num=123;
    var originlanumber=num;
    var reverse=0;
    var rem=0;

    while(num!=0){
      rem=num%10;
      reverse=reverse*10+rem
      num=num/10
    }
    println(reverse)
    if(reverse==originlanumber){
      print("palindroame number")
    }
    else {
      print("not a palindrome nuumber")
    }
}

}
