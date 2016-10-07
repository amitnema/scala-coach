package org.apn.hadoop.mapreduce.pattern.join;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apn.hadoop.commons.Constants;
import com.google.common.collect.Lists;

/**
 * 
 * The Class JoinReducer.</br>
 * Joins two record data-sets and produced the
 * 
 * record with the key first set of records & values as another set of records.
 * 
 */
public class JoinReducer extends Reducer<LongWritable, Text, Text, Text> {

	/** The Constant EMPTY_TEXT. */
	private static final Text EMPTY_TEXT = new Text("");

	/** The table1. */
	private List<Text> table1 = Lists.newArrayList();

	/** The table2. */
	private List<Text> table2 = Lists.newArrayList();

	/** The join type. */
	private String joinType;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.
	 * Reducer.Context)
	 */
	@Override
	protected void setup(Reducer<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		joinType = context.getConfiguration().get("type.join");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
			Reducer<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		table1.clear();
		table2.clear();
		for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
			String text = iterator.next().toString();
			char charAtIdx0 = text.toString().charAt(0);
			if (WeatherMapper.TAG == charAtIdx0) {
				table1.add(new Text(StringUtils.substring(text, 1)));
			} else if (ClimateMapper.TAG == charAtIdx0) {
				table2.add(new Text(StringUtils.substring(text, 1)));
			}
		}
		join(context);
	}

	/**
	 * Join.
	 *
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	private void join(Context context) throws IOException, InterruptedException {
		switch (joinType) {
		case Constants.JOIN_INNER:
			innerJoin(context);
			break;

		case Constants.JOIN_FULL:
			fullJoin(context);
			break;

		case Constants.JOIN_OUTER_RIGHT:
			rightOuterJoin(context);
			break;

		case Constants.JOIN_OUTER_LEFT:
			leftOuterJoin(context);
			break;

		case Constants.JOIN_ANTI:
			antiJoin(context);
			break;

		case Constants.JOIN_SEMI:
			semiJoin(context);
			break;

		default:
			break;
		}
	}

	/**
	 * Inner join.
	 *
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	private void innerJoin(Context context) throws IOException, InterruptedException {
		if (!table1.isEmpty() && !table2.isEmpty()) {
			for (Text valTab1 : table1) {
				for (Text valTab2 : table2) {
					context.write(valTab1, valTab2);
				}
			}
		}
	}

	/**
	 * Anti join.
	 *
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	private void antiJoin(Reducer<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		{
			// --records only from the left table(table1), which not exists in
			// right table(table2)
			if (!table1.isEmpty()) {
				if (table2.isEmpty()) {
					for (Text valTab1 : table1) {
						context.write(valTab1, EMPTY_TEXT);
					}
				}
			} else if (!table2.isEmpty()) {
				for (Text valTab2 : table1) {
					context.write(EMPTY_TEXT, valTab2);
				}
			}
		}
	}

	/**
	 * Semi join.
	 *
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	private void semiJoin(Reducer<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// --records only from the left table(table1), which also exists in
		// right table(table2)
		if (!table1.isEmpty()) {
			if (!table2.isEmpty()) {
				for (Text valTab1 : table1) {
					context.write(valTab1, EMPTY_TEXT);
				}
			}
		}
	}

	/**
	 * Left outer join.
	 *
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	private void leftOuterJoin(Reducer<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text valTab1 : table1) {
			// If table2 is not empty, then join both
			if (!table2.isEmpty()) {
				for (Text valTab2 : table2) {
					context.write(valTab1, valTab2);
				}
			} else {
				// Else, left table only (table1)
				context.write(valTab1, EMPTY_TEXT);
			}
		}
	}

	/**
	 * Right outer join.
	 *
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	private void rightOuterJoin(Reducer<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text valTab2 : table2) {
			// If table1 is not empty, then join both
			if (!table1.isEmpty()) {
				for (Text valTab1 : table1) {
					context.write(valTab1, valTab2);
				}
			} else {
				// Else, right table only (table2)
				context.write(valTab2, EMPTY_TEXT);
			}
		}
	}

	/**
	 * Full join.
	 *
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	private void fullJoin(Reducer<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// if table1 is not empty
		if (!table1.isEmpty()) {
			// iterate table1
			for (Text valTab1 : table1) {
				// If table2 is not empty, then join both
				if (!table2.isEmpty()) {
					for (Text valTab2 : table2) {
						context.write(valTab1, valTab2);
					}
				} else {
					// Else, left table only (table1)
					context.write(valTab1, EMPTY_TEXT);
				}
			}
		} else {
			// Else, right table only (table2)
			for (Text valTab2 : table2) {
				context.write(EMPTY_TEXT, valTab2);
			}
		}
	}

}