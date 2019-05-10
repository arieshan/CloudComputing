package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String s = value.toString(); // Converts Line to a String

		/* 
		 * TODO: emit: key:"Difference" value:difference calculated in DiffRed1
		 */
		
		Text outputKey = new Text("Difference");
		Text outputValue = new Text(s.split("\t")[1]);
		//System.out.println("key:"+outputKey.toString()+"\t"+outputValue.toString());
		context.write(outputKey, outputValue);
	}

}
