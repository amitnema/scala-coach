package org.apn.scala

/**
  * @author amit.nema
  */
class CurryingFunctions {

  def strcat(s1: String)(s2: String) = {
    s1 + s2
  }
}

object CurryingFunctions {
  def main(args: Array[String]): Unit = {
    execute()
    println((1 to 5) filter {
      _ % 2 == 0
    } map {
      _ * 2
    })

  }

  def execute() {
    val cf: CurryingFunctions = new CurryingFunctions()
    val str = cf.strcat("Hello,") _
    println(str("World!!!"))
    println(cf.strcat("Hello,")("World!!!"))
  }

  type R = Double

  def compose(g: R => R, h: R => R) = (x: R) => g(h(x))

  val f = compose({
    _ * 2
  }, {
    _ - 1
  })
}