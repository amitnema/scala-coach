package org.apn.hadoop.commons;

import java.text.SimpleDateFormat;

public interface Constants {
	SimpleDateFormat DATE_FRMT = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS");
	String JOIN_INNER = "JI";
	String JOIN_FULL = "JF";
	String JOIN_OUTER_RIGHT = "JOR";
	String JOIN_OUTER_LEFT = "JOL";
	String JOIN_ANTI = "JA";
	String JOIN_SEMI = "JS";
}
