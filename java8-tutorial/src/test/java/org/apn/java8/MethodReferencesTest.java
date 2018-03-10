package org.apn.java8;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

/**
 * The Class MethodReferencesTest.
 *
 * @author amit.nema
 */
@Test(enabled = false)
public class MethodReferencesTest {

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public static void main(String args[]) {
		List<String> names = new ArrayList<String>();

		names.add("India");
		names.add("Germany");
		names.add("United States");
		names.add("Singapore");
		names.add("Autralia");

		names.forEach(System.out::println);
	}
}
