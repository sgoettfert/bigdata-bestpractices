package com.bigdata.mapreduce.writable;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.bigdata.mapreduce.writable.UserWritable;


public class UserWritableTest {

	private static final Log LOG = LogFactory.getLog(UserWritableTest.class);
	
	public static final String DEL = "|";
	
	UserWritable user1, user2, user3, user4;
	List<UserWritable> userList;
	
	String tUser1 = "1" + DEL + "10" + DEL + "M" + DEL + "programmer" + DEL + "1010";
	String tUser2 = "1" + DEL + "20" + DEL + "M" + DEL + "programmer" + DEL + "1010";
	String tUser3 = "2" + DEL + "30" + DEL + "M" + DEL + "programmer" + DEL + "1010";
	
	private MapDriver<LongWritable, Text, LongWritable, UserWritable> mapDriver;
	private MapReduceDriver<LongWritable, Text, LongWritable, UserWritable, LongWritable, IntWritable> mrDriver;
	private ReduceDriver<LongWritable, UserWritable, LongWritable, IntWritable> redDriver;
	
	@Before
	public void setUp() {
		user1 = new UserWritable(1, 10, "M", "programmer", "1010");
		user2 = new UserWritable(1, 10, "M", "programmer", "1010");
		user3 = new UserWritable(1, 20, "F", "programmer", "1010");
		user4 = new UserWritable(1, 40, "M", "programmer", "1010");
		
		userList = new LinkedList<UserWritable>();
		
		mapDriver = new MapDriver<LongWritable, Text, LongWritable, UserWritable>()
				.withMapper(new UserWritableMapper());
		mrDriver = new MapReduceDriver<LongWritable, Text, LongWritable, UserWritable, LongWritable, IntWritable>()
				.withMapper(new UserWritableMapper())
				.withReducer(new UserWritableReducer());
		redDriver = new ReduceDriver<LongWritable, UserWritable, LongWritable, IntWritable>()
				.withReducer(new UserWritableReducer());
		
	}
	
	@Test
	public void testEquals() {
		assertTrue("User 1 != User 2", user1.equals(user2));
		assertFalse("User 1 == User 3", user1.equals(user3));
		assertFalse("User 1 == User 4", user1.equals(user4));
		assertFalse("User 3 == User 4", user3.equals(user4));
	}
	
	@Test
	public void testHashCode() {
		assertTrue("User 1 != User 2", user1.hashCode() == user2.hashCode());
		assertFalse("User 1 != User 3", user1.hashCode() == user3.hashCode());
		assertFalse("User 1 != User 4", user1.hashCode() == user4.hashCode());
		assertFalse("User 3 != User 4", user3.hashCode() == user4.hashCode());
	}
	
	@Test
	public void testReadFields() {
		
		assertTrue("", true);
	}
	
	@Test
	public void testToString() {
		assertTrue("User 1 != User 2", user1.toString().equals(user2.toString()));
		assertFalse("User 1 != User 3", user1.toString().equals(user3.toString()));
		assertFalse("User 1 != User 4", user1.toString().equals(user4.toString()));
	}
	
	@Test
	public void testWrite() {
		
		assertTrue("", true);
	}
	
	@Test
	public void testMap() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text(tUser1))
				.withOutput(new LongWritable(1), user1)
				.runTest();
	}
	
	@Test
	public void testReduce() throws IOException {
		userList.add(user1);
		userList.add(user3);
		userList.add(user4);
		
		assertTrue("List should have 3 elements!", userList.size() == 3);
		
		redDriver.withInput(new LongWritable(1), userList)
				.withOutput(new LongWritable(1), new IntWritable(70))
				.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		mrDriver
			.withInput(new LongWritable(1), new Text(tUser1))
			.withInput(new LongWritable(1), new Text(tUser2))
			.withOutput(new LongWritable(1), new IntWritable(30))
			.runTest();
	}
	
	public class UserWritableMapper extends Mapper<LongWritable, Text, LongWritable, UserWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] values = value.toString().split("\\|");
			
			if(values.length == 5) {
				context.write(
						new LongWritable(Long.parseLong(values[0])),
						new UserWritable(
								// userId, age
								Long.parseLong(values[0]), Integer.parseInt(values[1]),
								// gender, occupation, zip
								values[2], values[3], values[4]
								)
						);
			} else {
				LOG.error("Record with key " + key.toString() + " has invalid number of fields.\n");
			}
		}
	}
	
	public class UserWritableReducer extends Reducer<LongWritable, UserWritable, LongWritable, IntWritable> {
		
		@Override
		public void reduce(LongWritable key, Iterable<UserWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int age = 0;
			
			for(UserWritable value : values) {
				LOG.info("Age of current: " + value.get(1).toString());
				age += Integer.parseInt(value.get(1).toString());
			}

			context.write(key, new IntWritable(age));
		}
	}
	
}
