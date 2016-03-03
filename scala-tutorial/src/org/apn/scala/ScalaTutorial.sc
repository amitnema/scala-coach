package org.apn

object scalaws {

type R = Double
  def compose(g: R => R, h: R => R) = (x: R) => g(h(x))
                                                  //> compose: (g: org.apn.scalaws.R => org.apn.scalaws.R, h: org.apn.scalaws.R =>
                                                  //|  org.apn.scalaws.R)org.apn.scalaws.R => org.apn.scalaws.R
  val f = compose({ _ * 2 }, { _ - 1 })           //> f  : org.apn.scalaws.R => org.apn.scalaws.R = <function1>

  import sys.process._
  val status1 = "cmd dir"! //Also works           //> Microsoft Windows [Version 6.3.9600]
                                                  //| (c) 2013 Microsoft Corporation. All rights reserved.
                                                  //| 
                                                  //| D:\Tools\E\eclipse-jee-mars-R-win32-x86_64\eclipse>status1  : Int = 0

  val tuple = (1, "Scala Training", true)         //> tuple  : (Int, String, Boolean) = (1,Scala Training,true)

  var tup2 = (0, 0)                               //> tup2  : (Int, Int) = (0,0)
  main()                                          //> (0,0)

  def main(): Unit = {

    println(tup2)
  }                                               //> main: ()Unit

  def swapAdjacent(array: Array[Int]) = {
    for (i <- 0 until array.length) yield (
      if (i % 2 == 0)
        if (i == array.length - 1) array(i) else array(i + 1)
      else array(i - 1))
  }                                               //> swapAdjacent: (array: Array[Int])scala.collection.immutable.IndexedSeq[Int]
                                                  //| 

  def createArrayRandom(numberOfElements: Int): Array[Int] = {
    var arrRandom: Array[Int] = new Array(numberOfElements)
    var i = 0;
    while (i < numberOfElements) {
      //arrRandom(i) = Random.nextInt(10)
      i += 1;
    }
    arrRandom

  }                                               //> createArrayRandom: (numberOfElements: Int)Array[Int]

  def quickSort(xs: Array[Int]): Array[Int] = {
    if (xs.length <= 1) xs
    else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        quickSort(xs filter (pivot >)),
        xs filter (pivot ==),
        quickSort(xs filter (pivot <)))
    }
  }                                               //> quickSort: (xs: Array[Int])Array[Int]

  def bubbleSort(xs: Array[Int]): Array[Int] = {
    val n = xs.length;
    var k = 0
    var m = n;
    for (m <- n to 0) {

      for (i <- 0 to n - 1) {
        k = i + 1;
        if (xs(i) > xs(k)) {
          swapNumbers(i, k, xs);
        }
      }
    }
    xs

  }                                               //> bubbleSort: (xs: Array[Int])Array[Int]

  def swapNumbers(i: Int, j: Int, array: Array[Int]) {
    var temp = 0
    temp = array(i);
    array(i) = array(j);
    array(j) = temp;
  }                                               //> swapNumbers: (i: Int, j: Int, array: Array[Int])Unit

  def printNumbers(input: Array[Int]) {
    for (x <- 0 to input.length - 1) {
      print(input(x) + ", ")
    }
    println()

  }                                               //> printNumbers: (input: Array[Int])Unit

}