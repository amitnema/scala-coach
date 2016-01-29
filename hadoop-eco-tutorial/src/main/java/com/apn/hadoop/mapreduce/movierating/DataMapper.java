package com.apn.hadoop.mapreduce.movierating;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<Object, Text, IntWritable, Text> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// Get get each line
		String user[] = value.toString().split("\\s+");
		Integer userId = Integer.parseInt(user[0]);
		Integer movie = Integer.parseInt(user[1]);
		Integer rating = Integer.parseInt(user[2]);
		String val = movie + "|" + rating;
		// System.out.println("Writing data: "+movie);
		context.write(new IntWritable(userId), new Text(val));
	}
}