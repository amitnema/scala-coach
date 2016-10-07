package org.apn.hadoop.mapreduce.pattern.filter;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;

/**
 * The Class FilterDriver.
 */
public class FilterDriver extends Configured implements Tool {

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

		Preconditions.checkArgument(args.length == 2, "Usage: FilterDriver <input path> <output path>");

		final Job job = Job.getInstance(new Configuration(), "apn" + RandomStringUtils.random(9));
		job.setJarByClass(FilterDriver.class);

		job.setMapperClass(FilterMapper.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		//--Just to sort the record on the basis of key
		//job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String... args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new FilterDriver(), args));
	}
}
