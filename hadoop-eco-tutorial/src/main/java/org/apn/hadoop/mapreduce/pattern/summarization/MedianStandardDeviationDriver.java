package org.apn.hadoop.mapreduce.pattern.summarization;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;

/**
 * The Class MedianStandardDeviationDriver.
 */
public class MedianStandardDeviationDriver extends Configured implements Tool {

	/**
	 * Run.
	 *
	 * @param args
	 *            the args
	 * @return the int
	 * @throws Exception
	 *             the exception
	 */
	public int run(final String[] args) throws Exception {
		Preconditions.checkArgument(args.length == 2,
				"Usage: MedianStandardDeviationDriver <input path> <output path>");

		final Job job = Job.getInstance(this.getConf(), "APN-" + RandomStringUtils.randomAlphanumeric(9));
		job.setJarByClass(MedianStandardDeviationDriver.class);

		job.setMapperClass(MedianStandardDeviationMapper.class);
		job.setReducerClass(MedianStandardDeviationReducer.class);
		//job.setCombinerClass(MedianStandardDeviationCombiner.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MedianStandardDeviationTuple.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MedianStandardDeviationDriver(), args));
	}
}