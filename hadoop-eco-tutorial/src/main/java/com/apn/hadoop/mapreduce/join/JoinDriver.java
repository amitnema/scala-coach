package com.apn.hadoop.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.apn.hadoop.mapreduce.commons.XmlInputFormat;
import com.apn.hadoop.mapreduce.maxtemp.MaxTemperatureDriver;

/**
 * The Class JoinDriver.
 *
 * @author amit.nema
 */
public class JoinDriver extends Configured implements Tool {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: JoinDriver <reputation path> <posts path> <join type> <output path>");
			System.exit(-1);
		}

		Configuration conf = getConf();
		conf.set("xmlinput.start", "<posts>");
		conf.set("xmlinput.end", "</posts>");

		Job job = Job.getInstance(conf, "Get Reputation Posts");
		job.setJarByClass(getClass());

		MultipleInputs.addInputPath(job, new Path(args[0]), XmlInputFormat.class, ReputationMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), XmlInputFormat.class, PostsMapper.class);

		job.setReducerClass(JoinReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.getConfiguration().set("join.type", args[2]);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

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
		int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
		System.exit(exitCode);
	}
}