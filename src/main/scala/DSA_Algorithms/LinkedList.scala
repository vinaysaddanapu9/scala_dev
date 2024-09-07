package DSA_Algorithms

class Node[T](var data: T, var next: Option[Node[T]] = None)

class LinkedList[T] {
  var head: Option[Node[T]] = None

  def append(data: T): Unit = {
    val newNode = new Node(data)

    if (head.isEmpty) {
      head = Some(newNode)
    } else {

      var current = head
      while (current.get.next.isDefined) {
        current = current.get.next
      }
      current.get.next = Some(newNode)

    }
  }

  def display(): Unit = {
    var current = head
    while (current.isDefined) {
      print(current.get.data + " -> ")
      //      print(current.get.next + " -> ")
      current = current.get.next
    }
    //    println("null")
  }

  //  def delete(data: T): Unit = {
  //    head = deleteNode(head, data)
  //  }
  //
  //  private def deleteNode(node: Option[Node[T]], data: T): Option[Node[T]] = {
  //    var current = node
  //    var prev: Option[Node[T]] = None
  //
  //    while (current.isDefined && current.get.data != data) {
  //      prev = current
  //      current = current.get.next
  //    }
  //
  //    if (prev.isEmpty) {
  //      if (current.isDefined) {
  //        current = current.get.next
  //      }
  //    } else {
  //      if (current.isDefined) {
  //        prev.get.next = current.get.next
  //      } else {
  //        prev.get.next = None
  //      }
  //    }
  //
  //    node
  //  }


}

object LinkedList {

  def main(args: Array[String]): Unit = {
    val linkedList = new LinkedList[Int]

    linkedList.append(10)
    linkedList.append(20)
    linkedList.append(30)

    println("Linked List:")
    linkedList.display()
  }
}
