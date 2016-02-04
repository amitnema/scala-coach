package org.apn

object Returns {
  //  def getFive(): (Int, Int) = (1, 5)
  def getFive = (1, 5)
  def main(args: Array[String]) = {
    val (_, b) = getFive
    println("<<>>" + b);
  }
}