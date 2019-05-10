package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		/*
		 * TODO: For each value, emit: key:value, value:-rank
		 */
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			double rank = 0 - Double.parseDouble(key.toString());

			Text outputKey = it.next();
			Text outputValue = new Text(Double.toString(rank));

			context.write(outputKey, outputValue);
		}
	}
}
