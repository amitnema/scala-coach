package org.apn.hadoop.mapreduce.pattern.summarization;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apn.hadoop.commons.Constants;

/**
 * The Class MedianStandardDeviationMapper.
 */
public class MedianStandardDeviationMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger.getLogger(MedianStandardDeviationMapper.class);

	/** The out hour. */
	private final IntWritable outHour = new IntWritable();

	/** The out count. */
	private final IntWritable outCount = new IntWritable();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(final LongWritable key, final Text value,
			final Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {

		final String[] fields = StringUtils.split(value.toString(), '\t');
		final String strDate = fields[1];
		final String count = fields[2];
		Date creationDate;

		try {
			creationDate = Constants.DATE_FRMT.parse(strDate);
			outHour.set(creationDate.getHours());
		} catch (final ParseException e) {
			LOGGER.error(e);
		}

		outCount.set(NumberUtils.toInt(count));
		context.write(outHour, outCount);

	}

}
