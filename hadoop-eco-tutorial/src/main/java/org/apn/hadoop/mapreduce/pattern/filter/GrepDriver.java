package org.apn.hadoop.mapreduce.pattern.filter;

import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;

/**
 * The Class GrepDriver.
 */
public class GrepDriver extends Configured implements Tool {

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new GrepDriver(), args));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {

		Preconditions.checkArgument(args.length == 3, "Usage: GrepDriver <input path> <output path> <Regex Pattern>");

		Configuration conf = getConf();
		conf.set("mapregex", args[2]);

		final Job job = Job.getInstance(conf, "APN-" + RandomStringUtils.random(9));
		job.setJarByClass(GrepDriver.class);

		job.setMapperClass(GrepMapper.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// --Just to sort the record on the basis of key
		job.setNumReduceTasks(0);

		Path outputDir = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputDir);

		// Clean up empty output directory
		FileSystem.get(conf).delete(outputDir, true);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * The Class GrepMapper.
	 */
	public static class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {

		/** The map regex. */
		private String mapRegex = null;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
		 * Mapper.Context)
		 */
		public void setup(Context context) throws IOException, InterruptedException {
			mapRegex = context.getConfiguration().get("mapregex");
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			if (value.toString().toLowerCase().matches(mapRegex.toLowerCase())) {
				context.write(NullWritable.get(), value);
			}
		}
	}
}
