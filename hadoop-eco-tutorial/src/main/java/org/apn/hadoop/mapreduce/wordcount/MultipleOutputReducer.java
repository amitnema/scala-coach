package org.apn.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleOutputReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> multipleOutputs;

	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		multipleOutputs.write(key, new IntWritable(sum), key.toString().substring(0, 1));
	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
