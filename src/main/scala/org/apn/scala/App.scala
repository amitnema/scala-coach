package org.apn.scala

/**
  * @author amit.nema
  */
object App {
  var myVar: String = "Foo" //mutable variable
  val myVal: String = "Foo" //immutable variable

  /**
    * This will fold the elements in the direction from Left to Right.
    * So, the first element received by te function in first iteration is the FIRST element in the array.
    * e.g. in the below example, in first iteration a="" and b=&lt;value of the first element in the array&gt;
    *
    * @param x array
    * @return
    */
  def testFoldLeft(x: Array[String]): String = x.foldLeft("")((a, b) => a + b)

  /**
    * This will fold the elements in the direction from Right to Left.
    * So, the first element received by te function in first iteration is the LAST element in the array.
    * e.g. in the below example, in first iteration a="" and b=&lt;value of the last element in the array&gt;
    *
    * @param x array
    * @return
    */
  def testFoldRight(x: Array[String]): String = x.foldRight("")((a, b) => a + b)

  def main(args: Array[String]) {
    println("testFoldLeft = " + testFoldLeft(args))
    println("testFoldRight = " + testFoldRight(args))
  }

}
