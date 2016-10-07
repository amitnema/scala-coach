package org.apn.hadoop.mapreduce.pattern.summarization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;
import org.apn.hadoop.commons.Constants;

/**
 * The Class SummarizationTuple.
 */
public class SummarizationTuple implements Writable {

	/** The min. */
	private Date min = new Date();

	/** The max. */
	private Date max = new Date();

	/** The count. */
	private long count = 0;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// Write the data out in the order it is read,using the UNIX timestamp
		// to represent the Date
		out.writeLong(min.getTime());
		out.writeLong(max.getTime());
		out.writeLong(count);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// Read the data out in the order it is written,creating new Date
		// objects from the UNIX timestamp
		min = new Date(in.readLong());
		max = new Date(in.readLong());
		count = in.readLong();
	}

	/**
	 * Gets the min.
	 *
	 * @return the min
	 */
	public Date getMin() {
		return min;
	}

	/**
	 * Sets the min.
	 *
	 * @param min
	 *            the new min
	 */
	public void setMin(Date min) {
		this.min = min;
	}

	/**
	 * Gets the max.
	 *
	 * @return the max
	 */
	public Date getMax() {
		return max;
	}

	/**
	 * Sets the max.
	 *
	 * @param max
	 *            the new max
	 */
	public void setMax(Date max) {
		this.max = max;
	}

	/**
	 * Gets the count.
	 *
	 * @return the count
	 */
	public long getCount() {
		return count;
	}

	/**
	 * Sets the count.
	 *
	 * @param count
	 *            the new count
	 */
	public void setCount(long count) {
		this.count = count;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return Constants.DATE_FRMT.format(min) + "\t" + Constants.DATE_FRMT.format(max) + "\t" + count;
	}
}
