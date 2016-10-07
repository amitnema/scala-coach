package org.apn.hadoop.mapreduce.pattern.summarization;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apn.hadoop.commons.Constants;
import org.junit.Test;

/**
 * The Class SummarizationMapperTest.
 */
public class SummarizationMapperTest {

	/**
	 * Map.
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 * @throws ParseException
	 *             the parse exception
	 */
	@Test
	public void map() throws IOException, InterruptedException, ParseException {
		final Text value = new Text("Texas	2011-06-30T07:29:33.343	45");

		final Text outStateName = new Text();
		final String[] field = value.toString().split("\t");
		final String strDate = field[1];
		final String stateName = field[0];
		final SummarizationTuple outTuple = new SummarizationTuple();
		final Date creationDate = Constants.DATE_FRMT.parse(strDate);
		outTuple.setMin(creationDate);
		outTuple.setMax(creationDate);
		outTuple.setCount(1);
		outStateName.set(stateName);

		new MapDriver<LongWritable, Text, Text, SummarizationTuple>().withMapper(new SummarizationMapper())
				.withInput(new LongWritable(0), value).withOutput(outStateName, outTuple).runTest();
	}
}
