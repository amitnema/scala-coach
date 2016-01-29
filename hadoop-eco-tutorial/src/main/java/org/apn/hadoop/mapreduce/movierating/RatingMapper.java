package org.apn.hadoop.mapreduce.movierating;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String v[] = value.toString().split("\\s+");
		Integer movieId = Integer.parseInt(v[0]);
		String rating = v[1];

		context.write(new IntWritable(movieId), new Text(rating));
	}
}