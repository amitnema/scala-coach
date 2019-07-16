package org.apn.scala

/**
  * Print the list of files in a directory using cmd/shell
  *
  * @author amit.nema
  */
object ListFiles extends App {

  import sys.process.stringToProcess

  //val cmd = stringToProcess("ls")
  //val status = cmd! //Runs shell command
  val status = "ls -al" ! //Also works

}