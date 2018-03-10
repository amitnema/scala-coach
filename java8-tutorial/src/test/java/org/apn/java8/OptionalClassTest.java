package org.apn.java8;

import java.util.Optional;

import org.junit.Assert;
import org.testng.annotations.Test;

/**
 * The Class OptionalClassTest.
 *
 * @author amit.nema
 */
@Test
public class OptionalClassTest {

	/**
	 * Test optional class.
	 */
	public void testOptionalClass() {
		Integer value1 = null;
		Integer value2 = new Integer(10);

		// Optional.ofNullable - allows passed parameter to be null.
		Optional<Integer> a = Optional.ofNullable(value1);
		Assert.assertNotNull(a);
		Assert.assertFalse(a.isPresent());

		// Optional.of - throws NullPointerException if passed parameter is null
		Optional<Integer> b = Optional.of(value2);
		Assert.assertTrue(b.isPresent());

		// Optional.orElse - returns the value if present otherwise returns
		// the default value passed.
		Integer value11 = a.orElse(Integer.valueOf(0));
		Assert.assertEquals(0, value11.intValue());

		// Optional.get - gets the value, value should be present
		Integer value21 = b.get();
		Assert.assertEquals(10, value21.intValue());

		int intResult = value11 + value21;
		Assert.assertEquals(10, intResult);

	}
}
