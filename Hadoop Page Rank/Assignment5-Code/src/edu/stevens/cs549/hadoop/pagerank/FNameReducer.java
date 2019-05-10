package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FNameReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Text value, Context context) throws
		IOException, InterruptedException {
		String[] sections = value.toString().split("\t");
		String outKey = sections[0];
		String outVal = sections[1];
		
		context.write(new Text(outKey), new Text(outVal));
	}
}