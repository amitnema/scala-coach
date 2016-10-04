package org.apn.scala

import org.apn.scala.Location

object Demo {
  def main(args: Array[String]) {
    val loc = new Location(10, 20, 15);
    // Move to a new location 
    loc.move(10, 10, 5);
  }
}