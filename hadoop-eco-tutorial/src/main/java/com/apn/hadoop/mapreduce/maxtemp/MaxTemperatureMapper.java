package com.apn.hadoop.mapreduce.maxtemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//	@Override
//	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//		String line = value.toString();
//		String year = line.substring(15, 19);
//		int airTemperature = Integer.parseInt(line.substring(87, 92));
//		context.write(new Text(year), new IntWritable(airTemperature));
//	}
//}
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private NcdcRecordParser parser = new NcdcRecordParser();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		parser.parse(value);
		if (parser.isValidTemperature()) {
			context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
		}
	}
}