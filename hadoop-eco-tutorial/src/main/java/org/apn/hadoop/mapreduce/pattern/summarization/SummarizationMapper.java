package org.apn.hadoop.mapreduce.pattern.summarization;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apn.hadoop.commons.Constants;

/**
 * The Class SummarizationMapper.
 */
public class SummarizationMapper extends Mapper<LongWritable, Text, Text, SummarizationTuple> {

	/** The out state name. */
	private final Text outStateName = new Text();

	/** The out tuple. */
	// Tuple will hold all operations values
	private final SummarizationTuple outTuple = new SummarizationTuple();

	@Override
	protected void map(final LongWritable key, final Text value,
			final Mapper<LongWritable, Text, Text, SummarizationTuple>.Context context)
			throws IOException, InterruptedException {

		final String[] field = value.toString().split("\t");
		final String strDate = field[1];
		final String stateName = field[0];

		// Parse the string into a Date object
		Date creationDate;

		try {
			creationDate = Constants.DATE_FRMT.parse(strDate);

			// Set the minimum and maximum date values to the creationDate
			outTuple.setMin(creationDate);
			outTuple.setMax(creationDate);

		} catch (final ParseException e) {
			e.printStackTrace();
		}

		outTuple.setCount(1);
		outStateName.set(stateName);
		context.write(outStateName, outTuple);
	}
}
