package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/*
		 * TODO output key:-rank, value: node See IterMapper for hints on parsing the
		 * output of IterReducer.
		 */

		String[] sections = line.split("\t");

		if (sections.length > 2) {
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			// return;
		}

		String[] nodeRank = sections[0].split("\\+");
		String node = nodeRank[0];
		double rank = Double.parseDouble(nodeRank[1]);

		DoubleWritable outputKey = new DoubleWritable(0 - rank);
		Text outputValue = new Text(node);

		context.write(outputKey, outputValue);

	}

}
