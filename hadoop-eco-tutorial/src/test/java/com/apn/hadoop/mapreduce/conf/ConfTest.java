package com.apn.hadoop.mapreduce.conf;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class ConfTest {

	@Test
	public void testConfig() {
		Configuration conf = new Configuration();
		conf.addResource("com/apn/hadoop/mapreduce/configuration-1.xml");
		conf.addResource("com/apn/hadoop/mapreduce/configuration-2.xml");
		assertThat(conf.get("color"), is("yellow"));
		assertThat(conf.getInt("size", 0), is(12));
		assertThat(conf.get("breadth", "wide"), is("wide"));
		assertThat(conf.get("weight"), is("heavy"));
		assertThat(conf.get("size-weight"), is("12,heavy"));
		System.setProperty("size", "14");
		assertThat(conf.get("size-weight"), is("14,heavy"));
		System.setProperty("length", "2");
		assertThat(conf.get("length"), is((String) null));
	}
}
