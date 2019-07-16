package org.apn.scala

import scala.util.Random

/**
  * Create an array of random numbers and print them it in a sorted order
  */
object SortedArray {

  def main(args: Array[String]): Unit = {
    val xs = createArrayRandom(5)
    print("Input Array: ")
    printNumbers(xs)
    val arrXs: Array[Int] = bubbleSort(xs)
    print("Output Array: ")
    printNumbers(arrXs)
  }

  def createArrayRandom(numberOfElements: Int): Array[Int] = {
    var arrRandom: Array[Int] = new Array(numberOfElements)
    var i = 0
    while (i < numberOfElements) {
      arrRandom(i) = Random.nextInt(10)
      i += 1
    }
    arrRandom

  }

  def quickSort(xs: Array[Int]): Array[Int] = {
    if (xs.length <= 1) xs
    else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        quickSort(xs filter (pivot >)),
        xs filter (pivot ==),
        quickSort(xs filter (pivot <)))
    }
  }

  def bubbleSort(a: Array[Int]): Array[Int] = {
    for (i <- 1 to a.length - 1) {
      for (j <- (i - 1) to 0 by -1) {
        if (a(j) > a(j + 1)) {
          swapNumbers(j, j + 1, a)
        }
      }
    }
    a
  }

  def swapNumbers(i: Int, j: Int, array: Array[Int]) {
    var temp = 0
    temp = array(i)
    array(i) = array(j)
    array(j) = temp
  }

  def printNumbers(input: Array[Int]) {
    for (x <- 0 to input.length - 1) {
      print(input(x) + ", ")
    }
    println()

  }
}