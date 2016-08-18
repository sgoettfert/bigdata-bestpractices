package com.bigdata.mapreduce.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String outVal = "";
		
		for(Text value : values) {
			outVal += "," + value.toString();
		}
		
		outVal = outVal.substring(1);
		context.write(key, new Text(outVal));
	}
}
