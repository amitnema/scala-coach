package org.apn.spark;

import java.util.Map;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

class AgeCountTest {
	private static JavaSparkContext context = null;

	@BeforeClass
	private static void beforeClass() {
		final SparkConf conf = new SparkConf().setAppName(AgeCountTest.class.getName()).setMaster("local");
		context = new JavaSparkContext(conf);
	}

	@AfterClass
	private static void afterClass() {
		context.close();
	}

	@Test
	void testCountAge() {
		final String inpath = "target/test-classes/people.txt";
		final String outpath = "target/people-out";

		final JavaRDD<String> lines = context.textFile(inpath);

		final JavaPairRDD<Integer, Map<Integer, Integer>> pairs = AgeCount.countAge(lines);
		pairs.saveAsTextFile(outpath + new Random().nextInt());

	}

}
