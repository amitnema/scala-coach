package org.apn.java8;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class LambdaExpresssionTest {

	public void testLambdaExpresssion() {
		LambdaExpresssionTest tester = new LambdaExpresssionTest();

		// with type declaration
		MathOperation addition = (int a, int b) -> a + b;

		// with out type declaration
		MathOperation subtraction = (a, b) -> a - b;

		// with return statement along with curly braces
		MathOperation multiplication = (int a, int b) -> {
			return a * b;
		};

		// without return statement and without curly braces
		MathOperation division = (int a, int b) -> a / b;

		Assert.assertEquals(addition.operation(10, 5), 15);
		Assert.assertEquals(tester.operate(10, 5, addition), 15);
		Assert.assertEquals(tester.operate(10, 5, subtraction), 5);
		Assert.assertEquals(tester.operate(10, 5, multiplication), 50);
		Assert.assertEquals(tester.operate(10, 5, division), 2);

		// with parenthesis
		GreetingService greetService1 = message -> ("Hello " + message);

		// without parenthesis
		GreetingService greetService2 = (message) -> ("Hello " + message);

		Assert.assertEquals(greetService1.sayMessage("India"), "Hello " + "India");
		Assert.assertEquals(greetService2.sayMessage("US"), "Hello " + "US");
	}

	interface MathOperation {
		int operation(int a, int b);
	}

	interface GreetingService {
		String sayMessage(String message);
	}

	private int operate(int a, int b, MathOperation mathOperation) {
		return mathOperation.operation(a, b);
	}
}
