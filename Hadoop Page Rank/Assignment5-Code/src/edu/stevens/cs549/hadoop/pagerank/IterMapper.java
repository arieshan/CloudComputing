package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node+rank | Part 2: adj list

		if (sections.length > 2) // Checks if the data is in the incorrect format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {

			// return;
		}

		/*
		 * TODO: emit key: adj vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node! Put a marker on
		 * the string value to indicate it is an adjacency list.
		 */

		String[] adjVertex = null;
		if (sections.length == 2) {
			adjVertex = sections[1].split(" ");
		}

		double nodeRank = 0;

		nodeRank = Double.parseDouble(sections[0].split("\\+")[1]);

		int numOfAdjVertex = 0;
		if (adjVertex == null) {
			numOfAdjVertex = 0;
		} else {
			numOfAdjVertex = adjVertex.length;
		}

		if (adjVertex != null && adjVertex.length > 0) {
			for (int i = 0; i < numOfAdjVertex; i++) {
				Text outputKey = new Text(adjVertex[i]);
				Text outputValue = new Text(Double.toString((nodeRank / (double) numOfAdjVertex)));
				context.write(outputKey, outputValue);
			}

		}

		Text outputKey1 = new Text(sections[0].split("\\+")[0]);
		Text outputValue1 = null;
		if (sections.length != 2) {
			outputValue1 = new Text(PageRankDriver.MAKER + "");
		} else {
			outputValue1 = new Text(PageRankDriver.MAKER + sections[1]);
		}

		context.write(outputKey1, outputValue1);

	}

}
