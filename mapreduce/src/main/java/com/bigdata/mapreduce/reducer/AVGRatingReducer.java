package com.bigdata.mapreduce.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.bigdata.mapreduce.writable.LongPairWritable;

public class AVGRatingReducer extends Reducer<LongWritable, LongPairWritable, LongWritable, IntWritable>{
	
	@Override
	public void reduce(LongWritable key, Iterable<LongPairWritable> values, Context context)
			throws IOException, InterruptedException {
		
		long sum = 0L;
		long counter = 0L;
		
		for(LongPairWritable value : values) {
			sum += value.getFirst().get();
			counter += value.getSecond().get();
		}
		
		int avg = (int) (sum / counter);
		context.write(key, new IntWritable(avg));
	}

}
