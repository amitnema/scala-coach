package org.apn.scala

object Demo {
  def main(args: Array[String]) {
    val loc = new Location(10, 20, 15);
    // Move to a new location 
    loc.move(10, 10, 5);
  }
}