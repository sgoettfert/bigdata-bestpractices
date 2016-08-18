package com.bigdata.mapreduce.mapper;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.bigdata.mapreduce.mapper.ByUserMapper;
import com.bigdata.mapreduce.writable.LongArrayWritable;

public class ByUserMapperTest {
private static final String DELIMITER = "\t";
	
	private MapDriver<LongWritable, Text, LongWritable, LongArrayWritable> driver;
	private List<Pair<LongWritable, LongArrayWritable>> resultList;
	
	
	@Before
	public void setup() {
		driver = new MapDriver<LongWritable, Text, LongWritable, LongArrayWritable>()
				.withMapper(new ByUserMapper());
		
		resultList = new LinkedList<Pair<LongWritable,LongArrayWritable>>();
	}
	
	@Test
	public void testNormal() {
		long[] result = new long[] { 200L, 5L, 1400000000L };
		try {
			driver	// userId: 1000, movieId: 200, rating: 5, timestamp: 1400000000
					.withInput(new LongWritable(0L), new Text(1000 + DELIMITER + 200 + DELIMITER + 5 + DELIMITER + 1400000000))
					.withOutput(new LongWritable(1000), new LongArrayWritable(result))
					.runTest();
		} catch (IOException e) {
			fail();
		}
	}
	
	@Test
	public void testNormal_multiple() {
		try {
			resultList = driver	// { userId, movieId, rating, timestamp }
					.withInput(new LongWritable(0L), new Text(1000 + DELIMITER + 200 + DELIMITER + 4 + DELIMITER + 1400000000))
					.withInput(new LongWritable(44L), new Text(1000 + DELIMITER + 300 + DELIMITER + 5 + DELIMITER + 1400000000))
					.run();
			assertTrue("Result list should not be empty!", !resultList.isEmpty());
			assertTrue("Result list should contain 2 entries!", resultList.size() == 2);
		} catch (IOException e) {
			fail();
		}
	}
	
	@Test
	public void testIncomplete() {
		try {
			resultList = driver	// userId: 1000, movieId: 200, rating: null, timestamp: 1400000000
					.withInput(new LongWritable(0L), new Text(1000 + DELIMITER + 200 + DELIMITER + "null" + DELIMITER + 1400000000))
					.run();
			assertTrue("Result list should be empty!", resultList.isEmpty());
		} catch (IOException e) {
			fail();
		}
	}
}
