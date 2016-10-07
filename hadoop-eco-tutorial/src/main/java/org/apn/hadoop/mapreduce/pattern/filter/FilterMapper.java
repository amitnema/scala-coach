package org.apn.hadoop.mapreduce.pattern.filter;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The Class FilterMapper.
 */
public class FilterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	/** The outputs. */
	MultipleOutputs<LongWritable, Text> outputs;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		outputs = new MultipleOutputs<>(context);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String[] fields = StringUtils.split(value.toString(), '\t');
		String state = fields[0];
		String temprature = fields[2];
		if (NumberUtils.createInteger(temprature) > 20) {
			outputs.write(key, value, state + "/" + "part");
		}
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		outputs.close();
	}
}
