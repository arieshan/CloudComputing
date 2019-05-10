package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		/*
		 * TODO: The list of values should contain two ranks. Compute and output their
		 * difference.
		 */
		Iterator<Text> it = values.iterator();
		double diffResult = 0;

		int i = 0;

		while (it.hasNext()) {
			ranks[i] = Double.parseDouble(it.next().toString());
			i++;
			if (i > ranks.length) {
				throw new IOException("Incorrect data format");
			}
		}

		diffResult = Math.abs(ranks[0] - ranks[1]);

		Text outputKey = key;
		Text outputValue = new Text(Double.toString(diffResult));
		context.write(outputKey, outputValue);
	}
}
