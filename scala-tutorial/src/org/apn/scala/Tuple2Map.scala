package org.apn.scala
/**
 *
 * If a = Array(1,2,3,4) and b = Array("a", "b", "c", "d") generate the following output ((1,"a"),(2,"b"),(3,"c"),(4, "d")) and convert it to map
 *
 */
object Assign5 extends App {
  val a = Array(1, 2, 3, 4, 5)
  val b = Array("a", "b", "c", "d")
  var retTup: IndexedSeq[(Int, String)] = IndexedSeq()
  if (a.length <= b.length) {
    retTup = for (i <- 0 until a.length) yield {
      //map = Map(1->"a", 2->"b")
      a(i) -> b(i)
    }
  } else {
    retTup = for (i <- 0 until b.length) yield {
      (a(i), b(i))
    }
  }
  println(retTup)
  val retMap = retTup.toMap
  println(retMap)
}