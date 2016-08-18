package com.bigdata.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class LongPairWritable implements WritableComparable<LongPairWritable> {

	private LongWritable first;
	private LongWritable second;
	
	public LongPairWritable() {
		this.first = new LongWritable(0L);
		this.second = new LongWritable(0L);
	}
	
	public LongPairWritable(long first, long second) {
		this.first = new LongWritable(first);
		this.second = new LongWritable(second);
	}
	
	public LongPairWritable(LongWritable first, LongWritable second) {
		this.first = first;
		this.second = second;
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof LongPairWritable) {
			return this.compareTo((LongPairWritable) o) == 0;
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode(){
		return (int) (this.first.get() + this.second.get());
	}
	
	public int compareTo(LongPairWritable o) {
		if(o.getFirst().get() == this.first.get()) {
			if(o.getSecond().get() == this.second.get()) {
				return 0;
			} else {
				return o.getSecond().get() < this.second.get() ? 1 : -1;
			}
		} else {
			return o.getFirst().get() < this.first.get() ? 1 : -1;
		}
	}
	
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);		
	}
	
	@Override
	public String toString() {
		return this.first.get() + ":" + this.second.get();
	}

	public LongWritable getFirst() {
		return this.first;
	}
	
	public LongWritable getSecond() {
		return this.second;
	}
	
	public void setFirst(LongWritable first) {
		this.first = first;
	}
	
	public void setSecond(LongWritable second) {
		this.second = second;
	}
}
