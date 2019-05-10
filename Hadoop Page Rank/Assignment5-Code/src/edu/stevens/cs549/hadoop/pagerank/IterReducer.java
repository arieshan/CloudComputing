package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = PageRankDriver.DECAY; // Decay factor
		/* 
		 * TODO: emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		Iterator<Text> it = values.iterator();
		double nodeRank = 0;
		String adjVertex = null ;
		while(it.hasNext()) {
			Text nextText = it.next();
			
			if(nextText.toString().startsWith(PageRankDriver.MAKER)) {
				// or get the adj Vetex
				adjVertex = nextText.toString();
			} else {
				// accumulate the pageRank value
				nodeRank += Double.parseDouble(nextText.toString());
			}	
		}
		
		// 1-d + rank*d
		nodeRank = 1 - PageRankDriver.DECAY + nodeRank * PageRankDriver.DECAY;
		
		//key:node+rank, value: adjacency list
		String node = key.toString().split("\\+")[0];
		Text outputKey = new Text(node+"+"+nodeRank);
		Text outputValue = null;
		if(adjVertex == null) {
			System.out.println("+++++++++++");
			outputValue = new Text("");
		}else {
			outputValue = new Text(adjVertex.substring(4,adjVertex.length()));
		}
		
		System.out.println("key:"+outputKey.toString()+"\t"+"value:"+outputValue.toString());
			
		//}
		context.write(outputKey, outputValue);
	}
}
