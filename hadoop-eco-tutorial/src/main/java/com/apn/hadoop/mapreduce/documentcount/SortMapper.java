package com.apn.hadoop.mapreduce.documentcount;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for the dictionary sort task. This is an identity class which
 * simply outputs the key and the values that it gets, the intermediate key is
 * the document frequency of a word.
 * 
 * @author UP
 * 
 */
public class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String val = value.toString();
		if (val != null && !val.isEmpty() && val.length() >= 5) {
			String[] splits = val.split(",");
			context.write(new LongWritable(NumberUtils.toLong(splits[1])), new Text(splits[0] + "," + splits[2]));
		}
	}

}