package org.apn.hadoop.mapreduce.pattern.summarization;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Class MedianStandardDeviationCombiner.
 */
public class MedianStandardDeviationCombiner
		extends Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(final IntWritable key, final Iterable<SortedMapWritable> values,
			final Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable>.Context context)
			throws IOException, InterruptedException {
		final SortedMapWritable outValue = new SortedMapWritable();
		for (final SortedMapWritable v : values) {
			for (@SuppressWarnings("rawtypes")
			final Entry<WritableComparable, Writable> entry : v.entrySet()) {
				final LongWritable count = (LongWritable) outValue.get(entry.getKey());
				if (count != null) {
					count.set(count.get() + ((LongWritable) entry.getValue()).get());
				} else {
					outValue.put(entry.getKey(), new LongWritable(((LongWritable) entry.getValue()).get()));
				}
			}
		}
		context.write(key, outValue);
	}
}
