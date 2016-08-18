package com.bigdata.mapreduce.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BinningMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	private static final Log LOG = LogFactory.getLog(BinningMapper.class);
	
	private MultipleOutputs<LongWritable, Text> output;
	
	@Override
	public void setup(Context context) {
		output = new MultipleOutputs<LongWritable, Text>(context);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\\|");
		
		if(fields.length == 5) {
			output.write("occupation",
					new LongWritable(Long.parseLong(fields[1])),
					value,
					fields[3]);
		} else {
			LOG.info("Could not parse record '" + value.toString() + "'");
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		output.close();
	}
	
}
