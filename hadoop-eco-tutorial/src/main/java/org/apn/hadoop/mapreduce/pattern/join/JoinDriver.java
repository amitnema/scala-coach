package org.apn.hadoop.mapreduce.pattern.join;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;

public class JoinDriver extends Configured implements Tool {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		Preconditions.checkArgument(args.length > 3, "Usage: JoinDriver <input path1> <input path2> <output path>");
		Job job = Job.getInstance(new Configuration(), "HIVE: "+new Random().nextInt(10));

		job.setJarByClass(JoinDriver.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, WeatherMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ClimateMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.getConfiguration().set("type.join", args[3]);
		job.setReducerClass(JoinReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String... args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new JoinDriver(), args));
	}
}
