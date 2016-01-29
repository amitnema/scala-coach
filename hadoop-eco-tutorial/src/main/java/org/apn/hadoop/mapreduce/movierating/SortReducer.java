package org.apn.hadoop.mapreduce.movierating;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {

	@Override
	protected void reduce(FloatWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.write(values.iterator().next(), key);
	}
}