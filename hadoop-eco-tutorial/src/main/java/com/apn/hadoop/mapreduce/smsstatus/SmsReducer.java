package com.apn.hadoop.mapreduce.smsstatus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.apn.hadoop.mapreduce.commons.StringHandler;

/**
 * The Class SmsReducer.
 * 
 * @author amit.nema
 */
public class SmsReducer extends Reducer<Text, Text, Text, Text> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		HashMap<String, String> statusCodes = loadDeliveryStatusCodes();
		String custName = StringUtils.EMPTY;
		String deliveryReport = StringUtils.EMPTY;
		System.out.println("-----------------------------");
		for (Text value : values) {
			System.out.println(value);
			String[] tagCustNameDelivStatus = StringUtils
					.splitByWholeSeparator(StringUtils.trimToEmpty(value.toString()), "~");
			if (StringUtils.equals(tagCustNameDelivStatus[0], "CD")) {
				custName = StringUtils.defaultIfBlank(tagCustNameDelivStatus[1], "CustomerName");
			} else if (StringUtils.equals(tagCustNameDelivStatus[0], "DR")) {
				deliveryReport = statusCodes.getOrDefault(StringUtils.trimToEmpty(tagCustNameDelivStatus[1]),
						"DeliveryStatus");
			}
		}
		System.out.println("-----------------------------");
		context.write(new Text(custName), new Text(deliveryReport));
	}

	/**
	 * Load delivery status codes.
	 *
	 * @return the hash map
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	private HashMap<String, String> loadDeliveryStatusCodes() throws FileNotFoundException {
		InputStream inputStream = this.getClass().getResourceAsStream("DeliveryStatusCodes.txt");
		Scanner scanner = new Scanner(inputStream);
		HashMap<String, String> map = new HashMap<String, String>();

		while (scanner.hasNextLine()) {
			List<String> dscs = StringHandler.fromCommaSeparatedString(scanner.nextLine());
			map.put(dscs.get(0), dscs.get(1));
		}
		scanner.close();
		return map;
	}
}
