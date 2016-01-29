package org.apn.hadoop.mapreduce.join;

/**
 * The Enum JoinType.
 *
 * @author amit.nema
 */
public enum JoinType {

	/** The inner join. */
	INNER_JOIN("inner join"),
	/** The full join. */
	FULL_JOIN("full join"),
	/** The left outer join. */
	LEFT_OUTER_JOIN("left outer join"),
	/** The right outer join. */
	RIGHT_OUTER_JOIN("right outer join");

	/** The value. */
	private String value;

	/**
	 * Instantiates a new join type.
	 *
	 * @param value
	 *            the value
	 */
	private JoinType(String value) {
		this.value = value;
	}

	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public String getValue() {
		return value;
	}
}
