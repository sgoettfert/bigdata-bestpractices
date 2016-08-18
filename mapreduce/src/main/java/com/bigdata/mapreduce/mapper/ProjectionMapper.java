package com.bigdata.mapreduce.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.bigdata.mapreduce.writable.LongArrayWritable;

public class ProjectionMapper extends Mapper<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
	
	private static final Log LOG = LogFactory.getLog(FilterMapper.class);
	
	private int pos;
	
	@Override
	public void setup(Context context) {
		pos = context.getConfiguration().getInt("PROJECTION_POSITION", 0);
	}
	
	@Override
	public void map(LongWritable key, LongArrayWritable values, Context context)
			throws IOException, InterruptedException {

		LongWritable projection = values.get(pos);
		
		if(projection != null) {
			long[] value = new long[] {projection.get()};
			context.write(key, new LongArrayWritable(value));
		} else {
			LOG.info("Invalid projection for record with key: " + key.get());
		}
	}
	
}
