package org.apn.hadoop.mapreduce.documentcount;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * The main driver program for the sorting of the dictionary. This sorts the
 * dictionary by the document frequency of the words.
 * 
 * @author UP
 * 
 */
public class SortDriver {

	private static final Log logger = LogFactory.getLog(SortDriver.class);

	/**
	 * This is the main method that drives the creation of the inverted index.
	 * It expects the following input arguments - the location of the input
	 * files the location of the partition files
	 * 
	 * @param args
	 *            - the command line arguments
	 */
	public static void main(String[] args) {
		try {
			runJob(args[0], args[1], args[2], Integer.parseInt(args[3]));
		} catch (Exception ex) {
			logger.error(null, ex);
		}
	}

	/**
	 * This creates and runs the job for creating the inverted index
	 * 
	 * @param input
	 *            - location of the input folder
	 * @param output
	 *            - location of the output folder
	 * @param partitionLocation
	 *            - location of the partition folder
	 * @param numReduceTasks
	 *            - number of reduce tasks
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void runJob(String input, String output, String partitionLocation, int numReduceTasks)
			throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DictionarySorter");
		job.setJarByClass(SortDriver.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setNumReduceTasks(numReduceTasks);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass(SortKeyComparator.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output + ".dictionary.sorted." + getCurrentDateTime()));
		job.setPartitionerClass(TotalOrderPartitioner.class);

		Path inputDir = new Path(partitionLocation);
		Path partitionFile = new Path(inputDir, "partitioning");
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);

		double pcnt = 10.0;
		int numSamples = numReduceTasks;
		int maxSplits = numReduceTasks - 1;
		if (0 >= maxSplits)
			maxSplits = Integer.MAX_VALUE;

		InputSampler.Sampler sampler = new InputSampler.RandomSampler(pcnt, numSamples, maxSplits);
		InputSampler.writePartitionFile(job, sampler);

		try {
			job.waitForCompletion(true);
		} catch (InterruptedException ex) {
			logger.error(ex);
		} catch (ClassNotFoundException ex) {
			logger.error(ex);
		}
	}

	/**
	 * Returns todays date and time formatted as "yyyy.MM.dd.HH.mm.ss"
	 * 
	 * @return String - date formatted as yyyy.MM.dd.HH.mm.ss
	 */
	private static String getCurrentDateTime() {
		Date d = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
		return sdf.format(d);
	}

}