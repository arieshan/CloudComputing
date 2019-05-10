package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/*
		 * TODO: Just echo the input, since it is already in adjacency list format.
		 */

		String[] pairs = line.split(":");

		Text outputValue = null;
		if (pairs != null) {
			Text outputKey = new Text(pairs[0].trim());
			if (pairs.length == 2) {
				outputValue = new Text(pairs[1].trim());
			} else {
				outputValue = new Text("");
			}
			context.write(outputKey, outputValue);
		}

	}

}
