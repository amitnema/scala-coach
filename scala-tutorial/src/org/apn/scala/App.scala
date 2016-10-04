package org.apn.scala

/**
 * @author amit.nema
 */
object App {
  var myVar: String = "Foo" //mutable variable
  val myVal: String = "Foo" //immutable variable
  
  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    println("Hello World!")
    println("concat arguments = " + foo(args))
    println("Hello\tWorld\n\n");
  }

}
