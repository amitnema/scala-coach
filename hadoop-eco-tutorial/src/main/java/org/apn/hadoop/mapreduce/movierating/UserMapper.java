package org.apn.hadoop.mapreduce.movierating;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper<Object, Text, IntWritable, Text> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// Get get each line
		String user[] = value.toString().split("\\|");
		Integer userId = Integer.parseInt(user[0]);
		String occupation = user[3];
		context.write(new IntWritable(userId), new Text(occupation));
	}
}