package org.apn.scala

/**
 * Function that returns a tuple of minimum and maximum values of an array
 *
 */
object TupleMinMax extends App {

  println(tupleMinMax(Array(6, 5, 4, 9, -1, 8, 7)))

  def tupleMinMax(iArr: Array[Int] = Array()): (Int, Int) = {
    var minVal, maxVal = iArr(0)
    for (i <- iArr) {
      maxVal = math.max(i, maxVal)
      minVal = math.min(i, minVal)
    }
    (maxVal, minVal)
  }
}