package org.apn.java8;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class Streams {
	public static void main(String[] args) {
		List<String> strings = Arrays.asList("abc", "", "bc", "efg", "abcd", "", "jkl");
		// filter
		List<String> filtered = strings.stream().filter(string -> !string.isEmpty()).collect(Collectors.toList());
		filtered.forEach(System.out::println);
		// forEach
		Random random = new Random();
		random.ints().limit(10).forEach(System.out::println);
		// Map
		List<Integer> numbers = Arrays.asList(3, 2, 2, 3, 7, 3, 5);
		// get list of unique squares
		List<Integer> squaresList = numbers.stream().map(i -> i * i).distinct().collect(Collectors.toList());
		squaresList.forEach(System.out::println);
//		filter
		List<String> strings1 = Arrays.asList("abc", "", "bc", "efg", "abcd","", "jkl");
		//get count of empty string
		long count = strings1.stream().filter(string -> string.isEmpty()).count();
		System.out.println("count:"+count);
	}
}
