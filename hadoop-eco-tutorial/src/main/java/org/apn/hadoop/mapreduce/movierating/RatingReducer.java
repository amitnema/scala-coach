package org.apn.hadoop.mapreduce.movierating;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingReducer extends Reducer<IntWritable, Text, Text, Float> {

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// System.out.println("Key: "+key);
		// Start with empty value string
		int counter = 0;
		int score = 0;
		Text title = null;

		for (Text t : values) {
			if (isInteger(t.toString())) {
				counter++;
				score += Integer.parseInt(t.toString());
			} else {
				title = new Text(t);
			}
		}

		float avg = 0.00000000f;
		if (counter != 0) {
			avg = (float) score / (float) counter;
			context.write(title, avg);
			System.out.println("Score: " + score + " / " + counter + " = " + avg);
		}
	}

	private static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
		} catch (NumberFormatException e) {
			return false;
		}
		// only got here if we didn't return false
		return true;
	}
}