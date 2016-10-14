package org.apn.hadoop.mapreduce.pattern.summarization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * The Class MedianStandardDeviationTuple.
 */
public class MedianStandardDeviationTuple implements Writable {

	/** The median. */
	private FloatWritable median;

	/** The standard deviation. */
	private FloatWritable standardDeviation;

	/**
	 * Instantiates a new median standard deviation tuple.
	 */
	public MedianStandardDeviationTuple() {
		super();
		this.setMedian(new FloatWritable());
		this.setStandardDeviation(new FloatWritable());
	}

	/**
	 * Instantiates a new median standard deviation tuple.
	 *
	 * @param median
	 *            the median
	 * @param standardDeviation
	 *            the standard deviation
	 */
	public MedianStandardDeviationTuple(final IntWritable median, final IntWritable standardDeviation) {
		super();
		this.setMedian(new FloatWritable());
		this.setStandardDeviation(new FloatWritable());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		getMedian().write(out);
		getStandardDeviation().write(out);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(final DataInput in) throws IOException {
		getMedian().readFields(in);
		getStandardDeviation().readFields(in);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getMedian() + "\t" + getStandardDeviation();
	}

	/**
	 * Gets the median.
	 *
	 * @return the median
	 */
	public FloatWritable getMedian() {
		return median;
	}

	/**
	 * Sets the median.
	 *
	 * @param median
	 *            the new median
	 */
	public void setMedian(final FloatWritable median) {
		this.median = median;
	}

	/**
	 * Sets the median.
	 *
	 * @param median
	 *            the new median
	 */
	public void setMedian(final float median) {
		setMedian(new FloatWritable(median));
	}

	/**
	 * Gets the standard deviation.
	 *
	 * @return the standard deviation
	 */
	public FloatWritable getStandardDeviation() {
		return standardDeviation;
	}

	/**
	 * Sets the standard deviation.
	 *
	 * @param standardDeviation
	 *            the new standard deviation
	 */
	public void setStandardDeviation(final FloatWritable standardDeviation) {
		this.standardDeviation = standardDeviation;
	}

	/**
	 * Sets the standard deviation.
	 *
	 * @param standardDeviation
	 *            the new standard deviation
	 */
	public void setStandardDeviation(final float standardDeviation) {
		setStandardDeviation(new FloatWritable(standardDeviation));
	}
}
