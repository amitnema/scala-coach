package com.apn.hadoop.hive.udfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * The Class Strip.
 *
 * @author amit.nema
 */
public class Strip extends UDF {

	/** The result. */
	private Text result = new Text();

	/**
	 * Evaluate.
	 *
	 * @param str
	 *            the str
	 * @return the text
	 */
	public Text evaluate(Text str) {
		if (str != null) {
			result.set(new Text(StringUtils.strip(str.toString())));
		}
		return result;
	}

	/**
	 * Evaluate.
	 *
	 * @param str
	 *            the str
	 * @param stripChars
	 *            the strip chars
	 * @return the text
	 */
	public Text evaluate(Text str, String stripChars) {
		if (str != null) {
			result.set(new Text(StringUtils.strip(str.toString(), stripChars)));
		}
		return result;
	}
}
