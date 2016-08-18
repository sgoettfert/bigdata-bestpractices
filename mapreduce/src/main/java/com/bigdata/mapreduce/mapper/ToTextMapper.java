package com.bigdata.mapreduce.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.bigdata.mapreduce.writable.LongArrayWritable;

public class ToTextMapper extends Mapper<LongWritable, LongArrayWritable, LongWritable, Text> {
	
	private static final Log LOG = LogFactory.getLog(ToTextMapper.class);
	
	@Override
	public void map(LongWritable key, LongArrayWritable values, Context context)
			throws IOException, InterruptedException {

		context.write(key, new Text(values.toString()));
	}
}
