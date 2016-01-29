package com.apn.hadoop.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The Class HiveCreateTable.
 *
 * create database tpc_ds; 
 * use tpc_ds;
 * 
 *  hive --config /home/hduser/apn/config/hive --service hiveserver2
 *
 * @author amit.nema
 */
public class HiveCreateTable {

	/** The driver name. */
	private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public static void main(String[] args) {
		try {
			createTable();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates the table.
	 *
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 * @throws SQLException
	 *             the SQL exception
	 */
	private static void createTable() throws ClassNotFoundException, SQLException {
		// Register driver and create driver instance
		Class.forName(DRIVER_NAME);

		// get connection
		Connection con = DriverManager.getConnection("jdbc:hive2://192.168.15.136:10000/default", "hduser", "hadoop");

		// create statement
		Statement stmt = con.createStatement();

		// execute statement
		stmt.execute("CREATE TABLE IF NOT EXISTS " + " employee ( eid int, name String, "
				+ " salary String, destignation String)" + " COMMENT 'Employee details'" + " ROW FORMAT DELIMITED"
				+ " FIELDS TERMINATED BY '\t'" + " LINES TERMINATED BY '\n'" + " STORED AS TEXTFILE");

		System.out.println(" Table employee created.");
		
		stmt.close();
		con.close();
	}
}