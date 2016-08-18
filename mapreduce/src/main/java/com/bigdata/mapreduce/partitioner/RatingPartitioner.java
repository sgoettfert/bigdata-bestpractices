package com.bigdata.mapreduce.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.bigdata.mapreduce.writable.LongArrayWritable;

public class RatingPartitioner extends Partitioner<LongWritable, LongArrayWritable>{
	
	@Override
	public int getPartition(LongWritable key, LongArrayWritable record, int numPartitions) {

		/*
		 *  The rating value is always at position 1 and ranges from 1 to 5
		 *  Accordingly, the record is assigned to partition 0 to 4
		 */
		long rating = record.get(1).get();
		
		return (int) rating - 1;
	}
}