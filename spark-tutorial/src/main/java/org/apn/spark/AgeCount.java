package org.apn.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AgeCount {

	private static final Function<String, Map<Integer, Integer>> WORDS_EXTRACTOR = new Function<String, Map<Integer, Integer>>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Map<Integer, Integer> call(String v1) throws Exception {
			List<String> fields = Arrays.asList(v1.split(","));
			int age = Integer.parseInt(fields.get(2));
			int numFriends = Integer.parseInt(fields.get(3));
			Map<Integer, Integer> map = new HashMap<>();
			map.put(age, numFriends);
			return map;
		}

	};

	private static final PairFunction<Map<Integer, Integer>, Integer, Map<Integer, Integer>> WORDS_MAPPER = new PairFunction<Map<Integer, Integer>, Integer, Map<Integer, Integer>>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Map<Integer, Integer>> call(Map<Integer, Integer> t) throws Exception {
			return new Tuple2<Integer, Map<Integer, Integer>>(1, t);
		}
	};

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Please provide the input file full path as argument");
			System.exit(0);
		}

		SparkConf conf = new SparkConf().setAppName("org.apn.spark.WordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> lines = context.textFile(args[0]);

		JavaRDD<Map<Integer, Integer>> map = lines.map(WORDS_EXTRACTOR);
		JavaPairRDD<Integer, Map<Integer, Integer>> pairs = map.mapToPair(WORDS_MAPPER);
		pairs.saveAsTextFile(args[1] + new Random().nextInt());
		context.close();
	}

}
