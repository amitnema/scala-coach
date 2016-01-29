package com.apn.hadoop.hive.udafs;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

/**
 * The Class Maximum.
 *
 * @author amit.nema
 */
@SuppressWarnings("deprecation")
public class Maximum extends UDAF {

	/**
	 * The Class MaximumIntUDAFEvaluator.
	 */
	public static class MaximumIntUDAFEvaluator implements UDAFEvaluator {

		/** The result. */
		private IntWritable result;

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.hive.ql.exec.UDAFEvaluator#init()
		 */
		public void init() {
			result = null;
		}

		/**
		 * Iterate.
		 *
		 * @param value
		 *            the value
		 * @return true, if successful
		 */
		public boolean iterate(IntWritable value) {
			if (value == null) {
				return true;
			}
			if (result == null) {
				result = new IntWritable(value.get());
			} else {
				result.set(Math.max(result.get(), value.get()));
			}
			return true;
		}

		/**
		 * Terminate partial.
		 *
		 * @return the int writable
		 */
		public IntWritable terminatePartial() {
			return result;
		}

		/**
		 * Merge.
		 *
		 * @param other
		 *            the other
		 * @return true, if successful
		 */
		public boolean merge(IntWritable other) {
			return iterate(other);
		}

		/**
		 * Terminate.
		 *
		 * @return the int writable
		 */
		public IntWritable terminate() {
			return result;
		}
	}
}