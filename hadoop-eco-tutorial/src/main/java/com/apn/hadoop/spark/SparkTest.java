package com.apn.hadoop.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

public class SparkTest {
	private static final String APP_NAME = "RatingHistogram";
	private static final String MASTER = "local[4]";

	public static void main(String[] args) {
		new SparkTest().testSpark();
	}

	void testSpark() {
		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> distFile = sc.textFile("data.txt");
		// distFile.map(s -> s.length()).reduce((a, b) -> a + b);
		System.out.println(distFile);
	}
}
