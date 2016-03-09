package org.apn.scala

/**
 * Functionality for a binary search or half-interval search algorithm which finds the position of a target value within a sorted array.
 *
 * @author amit.nema
 */
object BinarySearch {

  def binarySearch(arrSearch: Array[Int], valToSearch: Int): Boolean = {
    if (arrSearch.length == 0) {
      return false;
    }
    val mid: Int = arrSearch.length / 2

    if (arrSearch(mid) > valToSearch) {
      binarySearch(arrSearch.slice(0, mid), valToSearch)
    } else if (arrSearch(mid) < valToSearch) {
      binarySearch(arrSearch.slice(mid + 1, arrSearch.length), valToSearch)
    } else {
      true
    }
  }

  def main(args: Array[String]): Unit = {
    val arrSearch: Array[Int] = Array(1, 2, 3, 4, 5)
    val valToSearch = 15;
    val b = binarySearch(arrSearch, valToSearch);
    println(b)
  }
}