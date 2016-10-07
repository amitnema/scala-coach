package org.apn.hadoop.mapreduce.pattern.summarization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

/**
 * The Class AverageTuple.
 */
public class AverageTuple implements Writable {

	/** The average. */
	private FloatWritable average;

	/** The count. */
	private FloatWritable count;

	/**
	 * Instantiates a new average tuple.
	 */
	public AverageTuple() {
		super();
		this.average = new FloatWritable();
		this.count = new FloatWritable();
	}

	/**
	 * Instantiates a new average tuple.
	 *
	 * @param average
	 *            the average
	 * @param count
	 *            the count
	 */
	public AverageTuple(FloatWritable average, FloatWritable count) {
		super();
		this.average = average;
		this.count = count;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		average.write(out);
		count.write(out);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		average.readFields(in);
		count.readFields(in);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return count + "\t" + average;
	}

	/**
	 * Gets the average.
	 *
	 * @return the average
	 */
	public FloatWritable getAverage() {
		return average;
	}

	/**
	 * Sets the average.
	 *
	 * @param average
	 *            the new average
	 */
	public void setAverage(FloatWritable average) {
		this.average = average;
	}

	/**
	 * Gets the count.
	 *
	 * @return the count
	 */
	public FloatWritable getCount() {
		return count;
	}

	/**
	 * Sets the count.
	 *
	 * @param count
	 *            the new count
	 */
	public void setCount(FloatWritable count) {
		this.count = count;
	}
}
