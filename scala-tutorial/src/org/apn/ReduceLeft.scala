package org.apn

object ReduceLeft extends App {
  val data = List(1, 2, 3, 4, 5);
  var sum = 0;
  //  for (i <- 0 to data.length - 1) {
  //    sum += data(i);
  //  }
  sum = data.reduceLeft(_ + _)
  println(sum); 
}