package org.apn.spark;

import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class WordCountTest {
	private static JavaSparkContext context = null;

	public void  testWordCount() {
		final String inpath = "target/test-classes/wordcount.txt";
		final String outpath = "target/wordcount-out";

		final JavaPairRDD<String, Integer> counter = WordCount.countWord(context, inpath);
		counter.saveAsTextFile(outpath + new Random().nextInt());

		context.close();
	}

	@BeforeClass
	public void beforeClass() {
		final SparkConf conf = new SparkConf().setAppName(AgeCountTest.class.getName()).setMaster("local");
		context = new JavaSparkContext(conf);
	}

	@AfterClass
	public void afterClass() {
		context.close();

	}

}
