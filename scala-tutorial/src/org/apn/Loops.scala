package org.apn

object Loops extends App {
  def addList(list: List[Int]): Int = {
    if (list.isEmpty) {
      return 0;
    }
    list.head + addList(list.tail)
  }
  val data = List(1, 2, 3, 4, 5);
  //  var sum, i = 0;
  //  while (i < data.length) {
  //    sum +=data(i);
  //    i += 1;
  //  }
  println(addList(data));
}