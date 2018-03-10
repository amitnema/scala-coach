package org.apn.java8;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * 
 * @author amit.nema
 *
 */
@Test
public class StreamsTest {
	static int check = 0;

	private static void count(final int i) {
		check++;
	}

	public void testStreams() {
		final List<String> strings = Arrays.asList("abc", "", "bc", "efg", "abcd", "", "jkl");

		// filter
		final List<String> filtered = strings.stream().filter(string -> !string.isEmpty()).collect(Collectors.toList());
		Assert.assertEquals(filtered, Arrays.asList("abc", "bc", "efg", "abcd", "jkl"));

		// forEach
		final Random random = new Random();
		final int LIMIT = 10;
		random.ints().limit(LIMIT).forEach(StreamsTest::count);
		Assert.assertEquals(check, LIMIT);

		// Map
		final List<Integer> numbers = Arrays.asList(3, 2, 2, 3, 7, 3, 5);
		// get list of unique squares
		final List<Integer> squaresList = numbers.stream().map(i -> i * i).distinct().collect(Collectors.toList());
		Assert.assertEquals(squaresList, Arrays.asList(9, 4, 49, 25));

		// filter
		final List<String> strings1 = Arrays.asList("abc", "", "bc", "efg", "abcd", "", "jkl");
		// get count of empty string
		final long count = strings1.stream().filter(string -> string.isEmpty()).count();
		Assert.assertEquals(count, 2);
	}
}
