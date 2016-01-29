package org.apn.hadoop.mapreduce.commons;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * The Class StringHandler.
 *
 * @author amit.nema
 */
public class StringHandler {

	/**
	 * To comma separated string.
	 *
	 * @param strings
	 *            the strings
	 * @return the string
	 */
	public static String toCommaSeparatedString(List<String> strings) {
		Joiner joiner = Joiner.on(",").skipNulls();
		return joiner.join(strings);
	}

	/**
	 * From comma separated string.
	 *
	 * @param string
	 *            the string
	 * @return the list
	 */
	public static List<String> fromCommaSeparatedString(String string) {
		Iterable<String> split = Splitter.on(",").omitEmptyStrings().trimResults().split(string);
		return Lists.newArrayList(split);
	}
}
