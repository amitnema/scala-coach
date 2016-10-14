package org.apn.hadoop.mapreduce.pattern.summarization;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

/**
 * The Class MedianStandardDeviationReducer.
 */
public class MedianStandardDeviationReducer
		extends Reducer<IntWritable, IntWritable, IntWritable, MedianStandardDeviationTuple> {

	/** The Constant EPSILON. */
	private static final double EPSILON = 0.00001;

	/** The median standard deviation tuple. */
	private final MedianStandardDeviationTuple medianStandardDeviationTuple = new MedianStandardDeviationTuple();

	/** The count list. */
	private final List<Float> countList = Lists.newArrayList();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(final IntWritable key, final Iterable<IntWritable> values,
			final Reducer<IntWritable, IntWritable, IntWritable, MedianStandardDeviationTuple>.Context context)
			throws IOException, InterruptedException {
		medianStandardDeviationTuple.setStandardDeviation(0);
		float sum = 0;
		float count = 0;

		for (final IntWritable value : values) {
			countList.add((float) value.get());
			sum += value.get();
			++count;
		}
		// -- sorted to calculate median
		Collections.sort(countList);

		// -- if count is an even value, average middle two elements
		if (Math.abs(count % 2) < EPSILON) {
			medianStandardDeviationTuple
					.setMedian((countList.get((int) count / 2 - 1) + countList.get((int) count / 2)) / 2.0f);
		} else {
			medianStandardDeviationTuple.setMedian(countList.get((int) count / 2));
		}

		// -- standard deviation
		final float mean = sum / count;
		float sumOfSquares = 0.0f;
		for (final Float x : countList) {
			sumOfSquares += (x - mean) * (x - mean);
			medianStandardDeviationTuple.setStandardDeviation((float) Math.sqrt(sumOfSquares / (count - 1)));
			context.write(key, medianStandardDeviationTuple);
		}
	}
}
