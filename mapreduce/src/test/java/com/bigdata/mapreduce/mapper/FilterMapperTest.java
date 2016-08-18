package com.bigdata.mapreduce.mapper;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.bigdata.mapreduce.mapper.FilterMapper;
import com.bigdata.mapreduce.writable.LongArrayWritable;

public class FilterMapperTest {

	private MapDriver<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> driver;
	private List<Pair<LongWritable, LongArrayWritable>> resultList;
	
	
	@Before
	public void setup() {
		driver = new MapDriver<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable>()
				.withMapper(new FilterMapper());
		
		resultList = new LinkedList<Pair<LongWritable,LongArrayWritable>>();
	}
	
	@Test
	public void testSingle_valid() {
		long[] record = new long[] { 200L, 5L, 1400000000L };
		try {
			driver.getConfiguration().setLong("FILTER_MIN_VAL", 100);
			driver.getConfiguration().setInt("FILTER_POSITION", 0);
			
			driver	// userId: 1000 | movieId: 200, rating: 5, timestamp: 1400000000
					.withInput(new LongWritable(200), new LongArrayWritable(record))
					.withOutput(new LongWritable(200), new LongArrayWritable(record))
					.runTest();
		} catch (IOException e) {
			fail();
		}
	}
	
	@Test
	public void testSingle_invalid() {
		long[] record = new long[] { 200L, 5L, 1400000000L };
		try {
			driver.getConfiguration().setLong("FILTER_MIN_VAL", 300);
			driver.getConfiguration().setInt("FILTER_POSITION", 0);
			
			resultList = driver	// userId: 1000 | movieId: 200, rating: 5, timestamp: 1400000000
					.withInput(new LongWritable(200), new LongArrayWritable(record))
					.run();
			
			assertTrue("Result list should be empty!", resultList.isEmpty());
		} catch (IOException e) {
			fail();
		}
	}
	
	@Test
	public void testNormal_multiple() {
		long[] record1 = new long[] { 1000L, 1L, 1400000000L };
		long[] record2 = new long[] { 2000L, 2L, 1400000000L };
		long[] record3 = new long[] { 3000L, 3L, 1400000000L };
		long[] record4 = new long[] { 4000L, 4L, 1400000000L };
		
		
		try {
			driver.getConfiguration().setLong("FILTER_MAX_VAL", 3L);
			driver.getConfiguration().setInt("FILTER_POSITION", 1);
			
			resultList = driver	// { userId, movieId, rating, timestamp }
					.withInput(new LongWritable(100), new LongArrayWritable(record1))
					.withInput(new LongWritable(200), new LongArrayWritable(record2))
					.withInput(new LongWritable(300), new LongArrayWritable(record3))
					.withInput(new LongWritable(400), new LongArrayWritable(record4))
					.run();
			assertTrue("Result list should not be empty!", !resultList.isEmpty());
			assertTrue("Result list should contain 3 entries!", resultList.size() == 3);
		} catch (IOException e) {
			fail();
		}
	}
	
	
}
