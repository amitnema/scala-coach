package org.apn.hadoop.mapreduce.hdfs;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class URLCat {

	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	public static void main(String[] args) throws Exception {
		InputStream in = null;
		try {
			String spec = args[0];
			System.out.println(spec);
			in = new URL(spec).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
}