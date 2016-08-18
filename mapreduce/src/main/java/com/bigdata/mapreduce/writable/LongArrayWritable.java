package com.bigdata.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class LongArrayWritable extends ArrayWritable {
	
	public LongArrayWritable() {
		super(LongWritable.class);
	}
	
	public LongArrayWritable(LongWritable[] values) {
		super(LongWritable.class, values);
	}
	
	public LongArrayWritable(long[] values) {
		super(LongWritable.class);
		LongWritable[] writables = new LongWritable[values.length];
		for(int i = 0; i < values.length; i++) {
			writables[i] = new LongWritable(values[i]);
		}
		
		this.set(writables);
	}
	
	public LongWritable get(int pos) {
		if(0 <= pos && pos <= this.get().length ) {
			Writable[] writables = this.get();
			return (LongWritable) writables[pos];
		} else {
			return null;
		}
	}
	
	/**
	 * Override Methods
	 */
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		LongWritable[] writables = new LongWritable[length];
		
		for(int i = 0; i < length; i++) {
			writables[i] = new LongWritable();
			writables[i].readFields(in);
		}
				
		set(writables);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.get().length);
		
		for(LongWritable writable : this.get()) {
			writable.write(out);
		}
	}
	
	@Override
	public LongWritable[] get() {
		return (LongWritable[]) super.get();
	}

	@Override
	public boolean equals(Object o) {
		LongArrayWritable other;
		boolean isEqual = false;
		
		try {
			other = (LongArrayWritable) o;
			isEqual = true;
		} catch(Exception e) {
			return isEqual;
		}
		
		if(this.get().length == other.get().length) {
			for(int i = 0; i < this.get().length; i++) {
				if(this.get(i).get() != other.get(i).get()) {
					isEqual = false;
				}
			}
		} else {
			isEqual = false;
		}
		
		return isEqual;
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
	
	@Override
	public String toString() {
		String s = "";
		int length = this.get().length;
		
		for(int i = 0; i < length - 1; i++) {
			s += this.get(i).get() + ",";
		}
		
		s += this.get(length - 1);
		
		return s;
	}
}
