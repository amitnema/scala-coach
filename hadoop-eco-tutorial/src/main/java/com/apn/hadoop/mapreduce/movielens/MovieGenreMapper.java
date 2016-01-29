package com.apn.hadoop.mapreduce.movielens;

import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author amit.nema
 *
 */
public class MovieGenreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text text = new Text();
	private final IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] genres = StringUtils.splitByWholeSeparator(value.toString(), "::");
		if (ArrayUtils.getLength(genres) > 2) {
			for (String genre : StringUtils.split(genres[2], "|")) {
				context.write(new Text(genre), ONE);
			}
		}
	}
}
