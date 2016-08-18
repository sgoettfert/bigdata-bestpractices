package com.bigdata.mapreduce.reducer;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AvroReducer extends Reducer<AvroKey<Long>, AvroValue<GenericRecord>, LongWritable, IntWritable> {

	@Override
	protected void reduce(AvroKey<Long> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
		long counter = 0;
		long sum = 0;
		
		for(AvroValue<GenericRecord> value : values) {
			GenericRecord record = value.datum();
			sum += (Integer) record.get("rating");
			counter++;
		}
		
		context.write(new LongWritable(key.datum()), new IntWritable((int) (sum / counter)));
		
	}
}
