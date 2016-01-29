package com.apn.hadoop.mapreduce.movielens;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author amit.nema
 *
 */

public class MovieMapper1 extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String v[] = value.toString().split("\\,");
		Integer movieId = 0;
		Double rating = 0.0;
		if (NumberUtils.isNumber(v[1])) {
			movieId = Integer.parseInt(v[1]);
			rating = Double.parseDouble(v[2]);
			Long timestamp = Long.valueOf(v[3]);
			Text val = new Text(timestamp + "$$$" + rating);
			context.write(new IntWritable(movieId), val);
		}
	}
}