package org.apn.hadoop.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.apn.hadoop.commons.Messages;

/**
 * The Class HiveUtils.
 *
 * <br />
 * <b>start hiveserver</b>
 * 
 * <br />
 * <code>hive --config /home/hduser/apn/config/hive --service hiveserver2</code>
 *
 * @author amit.nema
 */
public class HiveUtils {

	private static final Logger LOGGER = Logger.getLogger(HiveUtils.class);

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public static void main(final String[] args) {
		try {
			execute("drop table employee");
			final String sqlCreateTable = Messages.getString("hive.table.create");
			execute(sqlCreateTable);
			final String sqlLoadData = Messages.getString("hive.loaddata");
			execute(sqlLoadData);
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates the table.
	 * 
	 * @param sql
	 *
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 * @throws SQLException
	 *             the SQL exception
	 */
	private static void execute(final String sql) throws ClassNotFoundException, SQLException {
		// Register driver and create driver instance
		Class.forName(Messages.getString("hive.driver"));

		// get connection
		final Connection con = DriverManager.getConnection(Messages.getString("hive.url"), //$NON-NLS-1$
				Messages.getString("hive.user"), //$NON-NLS-1$
				Messages.getString("hive.password")); //$NON-NLS-1$

		// create statement
		final Statement stmt = con.createStatement();

		// execute statement
		stmt.execute(sql); // $NON-NLS-1$

		LOGGER.debug("Executed:" + sql);

		stmt.close();
		con.close();
	}
}