package com.bigdata.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

public class UserWritable extends TupleWritable {

	private Writable[] userVals;
	
	public UserWritable() {
		this(0, 0, "", "", "");
	}
	
	public UserWritable(long userId, int age, String gender, String occupation, String zip) {
		userVals = new Writable[] { new LongWritable(userId), new IntWritable(age),
						new Text(gender), new Text(occupation), new Text(zip) };
	}

	@Override
	public boolean equals(Object o) {
		UserWritable other;
		boolean isEqual = false;
		
		try {
			other = (UserWritable) o;
			isEqual = true;
			
			if(userVals.length == other.get().length) {
				for(int i = 0; i < this.get().length; i++) {
					if(! this.get(i).equals(other.get(i))) {
						isEqual = false;
					}
				}
			}
			
		} catch(Exception e) {
			isEqual = false;
		}		
		
		return isEqual;
	}
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// userId
		userVals[0] = new LongWritable();
		userVals[0].readFields(in);
		
		// age
		userVals[1] = new IntWritable();
		userVals[1].readFields(in);
		
		// gender, occupation, zip
		for(int i = 2; i < 5; i++) {
			userVals[i] = new Text();
			userVals[i].readFields(in);
		}
	}
	
	@Override
	public String toString() {
		String s = "";
		for(int i = 0; i < 5; i++) {
			s += userVals[i].toString() + "-";
		}
		
		return s.substring(0, s.length()-1);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		for(Writable writable : userVals) {
			writable.write(out);
		}
	}
	
	public Writable[] get() {
		return this.userVals;
	}
	
	public Writable get(int pos) {
		return userVals[pos];
	}
	
}
