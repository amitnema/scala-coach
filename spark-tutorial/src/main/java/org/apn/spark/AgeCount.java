package org.apn.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AgeCount {

	private static final Function<String, Map<Integer, Integer>> WORDS_EXTRACTOR = new Function<String, Map<Integer, Integer>>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Map<Integer, Integer> call(final String v1) throws Exception {
			final List<String> fields = Arrays.asList(v1.split(","));
			final int age = Integer.parseInt(fields.get(2));
			final int numFriends = Integer.parseInt(fields.get(3));
			final Map<Integer, Integer> map = new HashMap<>();
			map.put(age, numFriends);
			return map;
		}

	};

	private static final PairFunction<Map<Integer, Integer>, Integer, Map<Integer, Integer>> WORDS_MAPPER = new PairFunction<Map<Integer, Integer>, Integer, Map<Integer, Integer>>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Map<Integer, Integer>> call(final Map<Integer, Integer> t) throws Exception {
			return new Tuple2<Integer, Map<Integer, Integer>>(1, t);
		}
	};

	public static JavaPairRDD<Integer, Map<Integer, Integer>> countAge(final JavaRDD<String> lines) {
		final JavaPairRDD<Integer, Map<Integer, Integer>> pairs = lines.map(WORDS_EXTRACTOR).mapToPair(WORDS_MAPPER);
		return pairs;
	}

}
