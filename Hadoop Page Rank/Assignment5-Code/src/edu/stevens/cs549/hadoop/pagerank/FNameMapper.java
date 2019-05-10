package edu.stevens.cs549.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class FNameMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static HashMap<String, String> values = new HashMap<String, String>();

	@Override
	public void setup(Context context) throws IOException,InterruptedException{
		super.setup(context);
		URI[] files = context.getCacheFiles();

		for (URI file : files) {
			Path path = new Path(file);
			Configuration conf = new Configuration();

			FileSystem fs = path.getFileSystem(conf);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;

			line = bufferedReader.readLine();

			while (line != null) {
				System.out.println(line);
				String[] sections = line.split(":");
				String node = sections[0].trim();
				String linkName = sections[1].trim();
				values.put(node, linkName);
				line = bufferedReader.readLine();
			}
		}
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t");
		/*
		 * input: 
		 * emit key: rank, value: node
		 */
		String node = sections[0].trim(); // node
		String outVal = sections[1].trim(); // pageRank
		String outKey = values.get(node);

		context.write(new Text(outKey), new Text(outVal));

	}

}