package org.apn.hadoop.mapreduce.movielens;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author amit.nema
 *
 */
public class MovieGenreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	Text text = new Text();

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable intWritable : iterable) {
			sum += intWritable.get();
		}
		context.write(text, new IntWritable(sum));
	}
}
