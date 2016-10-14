package org.apn.hadoop.mapreduce.pattern.filter;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.math.NumberUtils;
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
 * The Class SimpleRandomSamplingDriver.
 */
public class SimpleRandomSamplingDriver extends Configured implements Tool {

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new SimpleRandomSamplingDriver(), args));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {

		Preconditions.checkArgument(args.length == 3,
				"Usage: SimpleRandomSamplingDriver <input path> <output path> <threshold percentage>");

		Configuration conf = getConf();
		conf.set("threshold_percentage", args[2]);

		final Job job = Job.getInstance(conf, "APN-" + RandomStringUtils.random(9));
		job.setJarByClass(SimpleRandomSamplingDriver.class);

		job.setMapperClass(SimpleRandomSamplingMapper.class);

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
	 * The Class SimpleRandomSamplingMapper.
	 */
	public static class SimpleRandomSamplingMapper extends Mapper<Object, Text, NullWritable, Text> {

		/** The random. */
		private Random random = new Random();

		/** The threshold. */
		private Double threshold;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
		 * Mapper.Context)
		 */
		public void setup(Context context) throws IOException, InterruptedException {
			threshold = NumberUtils.toDouble(context.getConfiguration().get("threshold_percentage"));
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
			if (random.nextDouble() < threshold) {
				context.write(NullWritable.get(), value);
			}
		}
	}
}
