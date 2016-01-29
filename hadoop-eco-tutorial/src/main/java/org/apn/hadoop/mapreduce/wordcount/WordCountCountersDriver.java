package org.apn.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
// Learning MapReduce by Nitesh Jain
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountCountersDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new WordCountCountersDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();

		Job job = Job.getInstance(configuration, "MyJob");
		job.setJarByClass(WordCountCountersDriver.class);
		job.setJobName("Word Count with Combiner.");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(WordCountCountersMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
