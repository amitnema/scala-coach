package org.apn.hadoop.mapreduce.pattern.summarization;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Class AverageReducer.
 */
public class AverageReducer extends Reducer<Text, AverageTuple, Text, AverageTuple> {

	/** The result. */
	private AverageTuple result = new AverageTuple();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<AverageTuple> values, Context context)
			throws IOException, InterruptedException {
		float sum = 0;
		float count = 0;

		// Iterate through all input values for this key
		for (AverageTuple val : values) {
			sum += val.getCount().get() * val.getAverage().get();
			count += val.getCount().get();
		}

		result.setCount(new FloatWritable(count));
		result.setAverage(new FloatWritable(sum / count));
		context.write(key, result);
	}
}