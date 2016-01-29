package com.apn.hadoop.hive.udfs;

import java.text.DateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * The Class UnixtimeToDate.
 *
 * @author amit.nema
 */
public class UnixtimeToDate extends UDF {

	/**
	 * Evaluate.
	 *
	 * @param text
	 *            the text
	 * @return the text
	 */
	public Text evaluate(Text text) {
		if (text == null)
			return null;
		long timestamp = Long.parseLong(text.toString());
		return new Text(toDate(timestamp));
	}

	/**
	 * To date.
	 *
	 * @param timestamp
	 *            the timestamp
	 * @return the string
	 */
	private String toDate(long timestamp) {
		Date date = new Date(timestamp * 1000);
		return DateFormat.getInstance().format(date).toString();
	}
}
