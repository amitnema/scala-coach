package org.apn.scala.sql

import java.sql.DriverManager

/**
 * class ensures that you will not create more than 10 connection objects?
 *
 */
class DBConnection(connectionId: String, url: String, port: Int) {
  var connection: java.sql.Connection = null
  val NUMBER_OF_CONNECTIONS = 10;
  var numberOfConn = 0
  private val props = Map(
    "url" -> url,
    "connectionId" -> connectionId,
    "port" -> port)

  def execute() {
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "root"

    try {
      // make the connection
      Class.forName(driver)
      if (numberOfConn < NUMBER_OF_CONNECTIONS) {
        connection = DriverManager.getConnection(url, username, password)
      } else {
        println("All " + NUMBER_OF_CONNECTIONS + " connections are busy.\n Please wait!!!")
      }
      println(s"Created new connection for " + props("url"))
      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT host, user FROM user")
      while (resultSet.next()) {
        val host = resultSet.getString("host")
        val user = resultSet.getString("user")
        println("host, user = " + host + ", " + user)
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }
}

object DBConnection extends App {
  // connect to the database named "mysql" on the localhost
  val connectionId: String = "retailDB"
  val url = "jdbc:mysql://127.0.0.1/mysql"
  val port: Int = 3306

  val connection: DBConnection = new DBConnection(connectionId, url, port)
  connection.execute()
} 