package com.bigdata.mapreduce.combiner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.bigdata.mapreduce.writable.LongPairWritable;

public class AVGRatingCombiner extends Reducer<LongWritable, LongPairWritable, LongWritable, LongPairWritable>{
	
	@Override
	public void reduce(LongWritable key, Iterable<LongPairWritable> values, Context context)
			throws IOException, InterruptedException {
		
		long sum = 0L;
		long counter = 0L;
		
		for(LongPairWritable value : values) {
			// array [rating, 1]
			sum += value.getFirst().get();
			counter += value.getSecond().get();
		}
		
		context.write(key, new LongPairWritable(sum, counter));
	}
}
