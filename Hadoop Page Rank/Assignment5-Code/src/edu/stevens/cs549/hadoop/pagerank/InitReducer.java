package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/*
		 * TODO: Output key: node+rank, value: adjacency list
		 */

		double initialRank = 1.0;
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			// output
			Text outputKey = new Text(key + "+" + initialRank);
			Text outputValue = it.next();

			context.write(outputKey, outputValue);
		}
	}
}
