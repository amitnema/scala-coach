package com.apn.hadoop.mapreduce.movierating;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String v[] = value.toString().split("\\|");
		Integer movieId = Integer.parseInt(v[0]);
		String title = v[1];

		context.write(new IntWritable(movieId), new Text(title));
	}
}