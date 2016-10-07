package org.apn.hadoop.mapreduce.pattern.join;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The Class WeatherMapper.
 */
public class WeatherMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	/** The Constant TAG. */
	protected static final char TAG = 'W';

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(final LongWritable key, final Text value,
			final Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		int fkeyIdx = 2;
		final String[] fields = StringUtils.split(value.toString(), '\t');
		context.write(new LongWritable(NumberUtils.createLong(fields[fkeyIdx])),
				new Text(TAG + value.toString()));
	}
}
