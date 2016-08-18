package com.bigdata.mapreduce.reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.bigdata.mapreduce.writable.LongArrayWritable;

public class InvertedIndexReducer extends Reducer<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable>{
	@Override
	protected void reduce(LongWritable key, Iterable<LongArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Set<Long> longs = new HashSet<Long>();
		
		for(LongArrayWritable array : values) {
			for(LongWritable value : array.get()) {
				longs.add(value.get());
			}
		}
		
		if(!longs.isEmpty()) {
			Object[] unique = longs.toArray();
			long[] result = new long[unique.length];
			
			for(int i = 0; i < unique.length; i++) {
				result[i] = (Long) unique[i];
			}
			
			context.write(key, new LongArrayWritable(result));
		}
	}
}
