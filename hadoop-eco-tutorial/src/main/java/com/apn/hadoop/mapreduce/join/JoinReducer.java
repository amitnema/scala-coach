package com.apn.hadoop.mapreduce.join;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

/**
 * The Class JoinReducer.
 *
 * @author amit.nema
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {

	private String joinType;
	private List<String> posts = Lists.newArrayList();
	private List<String> reputations = Lists.newArrayList();

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// Get the type of join from configuration
		joinType = context.getConfiguration().get("join.type");
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("-----------------------------");
		for (Text value : values) {
			System.out.println(value);
			String[] tagRepPost = StringUtils.splitByWholeSeparator(StringUtils.trimToEmpty(value.toString()), "~");
			if (StringUtils.equals(tagRepPost[0], "RD")) {
				reputations.add(StringUtils.defaultIfBlank(tagRepPost[1], "Reputation"));
			} else if (StringUtils.equals(tagRepPost[0], "PD")) {
				posts.add(StringUtils.defaultIfBlank(tagRepPost[1], "Post"));
			}
		}
		System.out.println("-----------------------------");
		executeJoin(key, context);
	}

	private void executeJoin(Text key, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Collection<?> collection = Lists.newArrayList();
		if (StringUtils.equals(joinType, JoinType.INNER_JOIN.getValue())) {
			collection = CollectionUtils.intersection(posts, reputations);
		} else if (StringUtils.equals(joinType, JoinType.FULL_JOIN.getValue())) {
			collection = CollectionUtils.union(posts, reputations);
		}

		for (Object object : collection) {
			context.write(key, new Text(object.toString()));
		}
	}

}
