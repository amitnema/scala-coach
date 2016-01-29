package com.apn.hadoop.mapreduce.movierating;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String v[] = value.toString().split(";");

		Float rating = Float.parseFloat(v[1]);
		String title = v[0];

		context.write(new FloatWritable(rating), new Text(title));
	}
}