package com.bigdata.mapreduce.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.bigdata.mapreduce.writable.LongArrayWritable;

public class FilterMapper extends Mapper<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
	
	public enum FILTERED_RECORDS { OUTDATED; };
	
	private static final Log LOG = LogFactory.getLog(FilterMapper.class);
	
	private int pos;
	private long max_val;
	private long min_val;
	
	@Override
	public void setup(Context context) {
		pos = context.getConfiguration().getInt("FILTER_POSITION", 0);
		max_val = context.getConfiguration().getLong("FILTER_MAX_VAL", Long.MAX_VALUE);
		min_val = context.getConfiguration().getLong("FILTER_MIN_VAL", Long.MIN_VALUE);
	}
	
	@Override
	public void map(LongWritable key, LongArrayWritable values, Context context)
			throws IOException, InterruptedException {

		if(valueInRange(values.get(pos).get())) {
			context.write(key, values);
		} else {
			context.getCounter(FILTERED_RECORDS.OUTDATED).increment(1);
			LOG.debug("Invalid record with ID: " + key.get());
		}
	}
	
	private boolean valueInRange(long value) {
		LOG.debug("MAX_VALUE: " + max_val);
		LOG.debug("MIN_VALUE: " + min_val);
		LOG.debug("VALUE: " + value);
		
		return (min_val <= value && value <= max_val);
	}
}
