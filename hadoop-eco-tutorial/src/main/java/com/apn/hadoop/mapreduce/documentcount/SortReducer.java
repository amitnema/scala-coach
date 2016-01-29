package com.apn.hadoop.mapreduce.documentcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for the dictionary sort class. This is an identity reducer
 * which simply outputs the values received from the map output.
 * 
 * @author UP
 *
 */
public class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		for (Text val : value) {
			context.write(new Text(val + "," + key), null);
		}
	}

}