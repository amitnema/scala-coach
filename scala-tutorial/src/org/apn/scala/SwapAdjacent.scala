package org.apn.scala

import scala.util.Random

/**
 * Write a function that takes an Int array input and swaps adjacent pairs of elements. Use for loop and yield
 *
 */
object SwapAdjacent {

  def main(args: Array[String]): Unit = {
    val iArr: Array[Int] = Array(1, 2, 3, 4, 5)
    val retVal = swapAdjacent(iArr)
    println(retVal)
  }

  def swapAdjacent(array: Array[Int]) = {
    for (i <- 0 until array.length) yield (
      if (i % 2 == 0)
        if (i == array.length - 1) array(i) else array(i + 1)
      else array(i - 1))
  }
}