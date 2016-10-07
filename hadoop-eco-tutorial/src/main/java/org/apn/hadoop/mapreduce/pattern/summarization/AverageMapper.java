package org.apn.hadoop.mapreduce.pattern.summarization;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apn.hadoop.commons.Constants;

/**
 * The Class AverageMapper.
 */
public class AverageMapper extends Mapper<LongWritable, Text, Text, AverageTuple> {

	private static final Logger LOGGER = Logger.getLogger(AverageMapper.class);
 
	/** The out hour. */
	private IntWritable outHour = new IntWritable();

	/** The out key. */
	private Text outKey = new Text();

	/** The out avg. */
	private AverageTuple outAvg = new AverageTuple();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(final LongWritable key, final Text value,
			final Mapper<LongWritable, Text, Text, AverageTuple>.Context context)
			throws IOException, InterruptedException {

		final String[] fields = StringUtils.split(value.toString(), '\t');
		final String stateName = fields[0];
		final String strDate = fields[1];
		final String count = fields[2];
		Date creationDate;

		try {
			creationDate = Constants.DATE_FRMT.parse(strDate);
			outHour.set(creationDate.getHours());
		} catch (final ParseException e) {
			LOGGER.error(e);
		}
		outAvg.setCount(new FloatWritable(1));
		outAvg.setAverage(new FloatWritable(Float.parseFloat(count)));
		outKey.set(stateName + "_" + String.valueOf(outHour.get()) + "H");
		context.write(outKey, outAvg);
	}
}