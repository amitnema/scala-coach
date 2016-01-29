package org.apn.hadoop.mapreduce.movierating;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// Start with empty value string
		String value = "";

		// Iterate through all values and add them together in a string
		for (Text t : values) {
			// only add to the value if already have something,
			// otherwise just set the new value
			if (!value.equals(""))
				value = value + "," + t;
			else
				value = t + "";
		}

		// Split the new value on comma for further processing
		String all[] = value.split(",");

		// Check if we need to use this particular instance
		// only accept users with occupation programmer, lawyer or retired
		// The occupation will be either the first or the last element
		if ((all[0].equals("programmer") || all[all.length - 1].equals("programmer"))
				|| (all[0].equals("lawyer") || all[all.length - 1].equals("lawyer"))
				|| (all[0].equals("retired") || all[all.length - 1].equals("retired"))) {
			// System.out.println("UserId: "+key);
			// System.out.println("UserValue: "+value);

			// So we have a relevant line, now get all the ratings and poop out
			for (String r : all) {
				// System.out.println("Valuestring: "+r);
				// There is at least one item without ratings so skipt that
				if (!r.equals("") && r.contains("|")) {
					String ratingitem[] = r.split("\\|");
					Integer movieId = Integer.parseInt(ratingitem[0]);
					Integer rating = Integer.parseInt(ratingitem[1]);
					// System.out.println("Movie: "+movieId+" Rating: "+rating);
					context.write(new IntWritable(movieId), new IntWritable(rating));
				}
			}
		}
	}
}