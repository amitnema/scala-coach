package org.apn.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text text, IntWritable intWritable, int numberOfReducer) {
		int i = 1;
		int charAt = text.charAt(0);
		if ((charAt < 'L') || (charAt > 'Z' && charAt < 'l')) {
			i = charAt % numberOfReducer;
		}
		return i;
	}
}