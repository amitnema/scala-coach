package org.apn.scala
/**
 * @author amit.nema
 */
object Loops extends App {
  def addList(list: List[Int]): Int = {
    if (list.isEmpty) {
      return 0;
    }
    list.head + addList(list.tail)
  }
  val data = List(1, 2, 3, 4, 5);
  println(addList(data));
}

object ReduceLeft extends App {
  val data = List(1, 2, 3, 4, 5);
  var sum = 0;
  sum = data.reduceLeft(_ + _)
  println(sum);
}