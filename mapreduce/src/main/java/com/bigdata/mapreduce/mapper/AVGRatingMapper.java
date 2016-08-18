package com.bigdata.mapreduce.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bigdata.mapreduce.parser.MovieParser;
import com.bigdata.mapreduce.parser.MovieRating;
import com.bigdata.mapreduce.writable.LongPairWritable;

public class AVGRatingMapper extends Mapper<LongWritable, Text, LongWritable, LongPairWritable>{
	
	public static enum RECORDS { MALFORMED, WELLFORMED;};
	
	private MovieParser parser = new MovieParser();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		MovieRating rating = parser.pars(value, null);
		
		if(rating != null) {
			context.write(
					new LongWritable(rating.getItemId()),
					new LongPairWritable(rating.getRating(), 1L));
			
			context.getCounter(RECORDS.WELLFORMED).increment(1);
		} else {
			context.getCounter(RECORDS.MALFORMED).increment(1);
		}
		
	}

}
