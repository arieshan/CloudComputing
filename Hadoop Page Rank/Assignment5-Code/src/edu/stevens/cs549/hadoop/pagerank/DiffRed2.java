package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed2 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double diff_max = 0.0; // sets diff_max to a default value
		/*
		 * TODO: Compute and emit the maximum of the differences
		 */
		Iterator<Text> it = values.iterator();

		while (it.hasNext()) {
			double currDiff = Double.parseDouble(it.next().toString());
			if (currDiff > diff_max) {
				diff_max = currDiff;
			}
		}

		Text outputKey = new Text("");
		Text outputValue = new Text(Double.toString(diff_max));
		context.write(outputKey, outputValue);
	}
}
