package org.apn.java8;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class DefaultMethodTest.
 *
 * @author amit.nema
 */
@Test
public class DefaultMethodTest {

	/**
	 * Test default methods.
	 */
	public void testDefaultMethods() {
		Vehicle vehicle = new Car();
		int wheels = vehicle.getWheels();
		Assert.assertEquals(wheels, 5, "I am a Four Wheeler. Must have default wheel 5, including spare wheel.");
		String blowHorn = Vehicle.blowHorn();
		Assert.assertEquals(blowHorn, Vehicle.BLOWING_HORN, "I am a car! mst blow horn.");
	}
}

interface Vehicle {
	public static final String BLOWING_HORN = "Blowing horn!!!";

	default int getWheels() {
		return 0;
	}

	static String blowHorn() {
		return BLOWING_HORN;
	}
}

interface FourWheeler {
	default int getWheels() {
		return 4;
	}
}

class Car implements Vehicle, FourWheeler {

	public int getWheels() {
		int wheels = Vehicle.super.getWheels();
		Assert.assertEquals(wheels, 0, "I am a vehicle. Must have default wheel 0.");
		wheels = FourWheeler.super.getWheels();
		Assert.assertEquals(wheels, 4, "I am a Four Wheeler. Must have default wheel 4.");
		// including spare wheel 4 + 1
		return 5;
	}

}