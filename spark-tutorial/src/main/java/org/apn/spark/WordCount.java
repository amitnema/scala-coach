package org.apn.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	private static final FlatMapFunction<String, String> WORDS_EXTRACTOR = new FlatMapFunction<String, String>() {
		private static final long serialVersionUID = 1L;

		public Iterator<String> call(final String s) throws Exception {
			return Arrays.asList(s.split(" ")).iterator();
		}
	};

	private static final PairFunction<String, String, Integer> WORDS_MAPPER = new PairFunction<String, String, Integer>() {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> call(final String s) throws Exception {
			return new Tuple2<String, Integer>(s, 1);
		}
	};

	private static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {
		private static final long serialVersionUID = 1L;

		public Integer call(final Integer a, final Integer b) throws Exception {
			return a + b;
		}
	};

	public static JavaPairRDD<String, Integer> countWord(final JavaSparkContext context, final String inpath) {
		final JavaRDD<String> file = context.textFile(inpath);
		final JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
		final JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
		final JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);
		return counter;
	}
}