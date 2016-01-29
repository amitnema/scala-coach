package org.apn.hadoop.mapreduce.movielens;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author amit.nema
 *
 */
public class MovieReducer1 extends Reducer<IntWritable, Text, IntWritable, DoubleWritable> {
	@Override
	protected void reduce(IntWritable intWritable, Iterable<Text> values,
			Reducer<IntWritable, Text, IntWritable, DoubleWritable>.Context context)
					throws IOException, InterruptedException {
		long maxId = Long.MIN_VALUE;
		String prevStrMaxVal = StringUtils.EMPTY;
		Double maxValue = Double.MIN_VALUE;
		for (Text value : values) {
			String[] split = StringUtils.splitByWholeSeparator(value.toString(), "$$$");
			maxId = Math.max(maxId, Long.valueOf(split[0]));
			if (StringUtils.equals(split[0], Long.toString(maxId))) {
				maxValue = Double.valueOf(split[1]);
			} else {
				String[] prevStrMaxVals = StringUtils.splitByWholeSeparator(prevStrMaxVal, "$$$");
				maxValue = Double.valueOf(prevStrMaxVals[1]);
			}
			prevStrMaxVal = value.toString();
		}
		context.write(intWritable, new DoubleWritable(maxValue));

	}
}
