package org.apn.java8;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.junit.Assert;
import org.testng.annotations.Test;

/**
 * The Class FunctionalInterfacesTest.
 *
 * @author amit.nema
 */
@Test
public class FunctionalInterfacesTest {

	/** The int result. */
	int intResult;

	/**
	 * Test predicate.
	 */
	@Test(enabled=false)
	public void testPredicate() {
		final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

		// Predicate<Integer> predicate = n -> true
		// n is passed as parameter to test method of Predicate interface
		// test method will always return true no matter what value n has.

		System.out.println("Print all numbers:");

		// pass n as parameter
		eval(list, n -> true);

		// Predicate<Integer> predicate1 = n -> n%2 == 0
		// n is passed as parameter to test method of Predicate interface
		// test method will return true if n%2 comes to be zero

		System.out.println("Print even numbers:");
		eval(list, n -> n % 2 == 0);

		// Predicate<Integer> predicate2 = n -> n > 3
		// n is passed as parameter to test method of Predicate interface
		// test method will return true if n is greater than 3.

		System.out.println("Print numbers greater than 3:");
		eval(list, n -> n > 3);

		System.out.println("------------------------------");
		System.out.println("BiConsumer Test");
	}

	/**
	 * Eval.
	 *
	 * @param list
	 *            the list
	 * @param predicate
	 *            the predicate
	 */
	private static void eval(final List<Integer> list, final Predicate<Integer> predicate) {
		for (final Integer n : list) {

			if (predicate.test(n)) {
				System.out.println(n + " ");
			}
		}
	}

	/**
	 * Test bi consumer.
	 */
	public void testBiConsumer() {
		final BiConsumer<Integer, Integer> biConsumer = (x, y) -> {
			intResult = x * y;
		};
		biConsumer.accept(4, 5);
		Assert.assertEquals(20, intResult);
	}

	/**
	 * Test bi function.
	 */
	public void testBiFunction() {
		final BiFunction<Integer, Integer, Integer> biFunction = (x, y) -> {
			return x * y;
		};
		intResult = biFunction.apply(4, 5);
		Assert.assertEquals(20, intResult);
	}

}
