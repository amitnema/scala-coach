package org.apn.hadoop.mapreduce.movierating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopMovies {

	private static final String OUTPUT_PATH = "movies/temp";
	private static final String OUTPUT_PATH2 = "movies/temp2";

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: TopMovies <user path> <data path> <movie path> <output path>");
			System.exit(-1);
		}

		Path temppath = new Path(OUTPUT_PATH);
		Path temppath2 = new Path(OUTPUT_PATH2);

		// Finish indexing unique searches
		Job job = Job.getInstance(new Configuration(), "Get Ratings");
		job.setJarByClass(TopMovies.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DataMapper.class);

		job.setReducerClass(MovieReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileOutputFormat.setOutputPath(job, temppath);

		job.waitForCompletion(true);

		if (job.isComplete() && job.isSuccessful()) {
			System.out.println("======= JOB 1 done succesful =====");
			job.killJob();
			@SuppressWarnings("deprecation")
			Job job2 = new Job(new Configuration(), "Get Movies");
			job2.setJarByClass(TopMovies.class);

			MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapper.class);
			MultipleInputs.addInputPath(job2, temppath, TextInputFormat.class, RatingMapper.class);

			job2.setReducerClass(RatingReducer.class);

			setTextoutputformatSeparator(job2, ";");
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Float.class);

			FileOutputFormat.setOutputPath(job2, temppath2);
			job2.waitForCompletion(true);

			if (job2.isComplete() && job2.isSuccessful()) {
				System.out.println("======= JOB 2 done succesful =====");
				job2.killJob();
				Job job3 = Job.getInstance(new Configuration(), "Sort Movies");
				job3.setJarByClass(TopMovies.class);

				FileInputFormat.addInputPath(job3, temppath2);

				job3.setMapperClass(SortMapper.class);
				job3.setReducerClass(SortReducer.class);

				job3.setSortComparatorClass(SortFloatComperator.class);
				job3.setMapOutputKeyClass(FloatWritable.class);
				job3.setMapOutputValueClass(Text.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(FloatWritable.class);

				FileOutputFormat.setOutputPath(job3, new Path(args[3]));
				job3.waitForCompletion(true);
				System.out.println("======= JOB 3 done =====");
				System.out.println("======= PROGRAM FINISHED =====");
			}
		}
	}

	private static void setTextoutputformatSeparator(final Job job, final String separator) {
		final Configuration conf = job.getConfiguration(); // ensure accurate
															// config ref

		conf.set("mapred.textoutputformat.separator", separator); // Prior to
																	// Hadoop 2
																	// (YARN)
		conf.set("mapreduce.textoutputformat.separator", separator); // Hadoop
																		// v2+
																		// (YARN)
		conf.set("mapreduce.output.textoutputformat.separator", separator);
		conf.set("mapreduce.output.key.field.separator", separator);
		conf.set("mapred.textoutputformat.separatorText", separator); // ?
	}
}