package org.apn.scala

object PartiallyAppliedFunctions {

  def modulo(dividend: Int, divisor: Int): Int = dividend % divisor

  def main(args: Array[String]): Unit = {
    val funcModulo2 = modulo(_: Int, 2)
    val funcModulo3 = modulo(_: Int, 3)
    println(funcModulo2(9999))
    println(funcModulo3(9999))
  }
  
}