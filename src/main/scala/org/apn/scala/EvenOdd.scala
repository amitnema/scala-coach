package org.apn.scala

/**
  * @author amit.nema
  */

/**
  * Print list of even numbers and odd numbers from an array.
  */
object EvenOdd {

  def main(args: Array[String]): Unit = {
    val arr: List[Int] = List(9, 7, 5, 6, 8, 0, 5, 4, 3, 2, 1)
    var evenList: List[Int] = List()
    var oddList: List[Int] = List()
    for (i <- arr) {
      if ((arr(i) % 2) == 0) {
        evenList = evenList.::(arr(i))
      } else {
        oddList = oddList.::(arr(i))
      }
    }
    println(arr)
    println(evenList)
    println(oddList)
  }
}