package com.bigdata.mapreduce.parser;

import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

public class MovieParser {

	public static final String DEFAULT_DELIMITER = "\t";
	
	public MovieRating pars(String record, String delimiter) {
		
		StringTokenizer tokenizer;
		
		if(delimiter != null) {
			tokenizer = new StringTokenizer(record, delimiter);
		} else {
			tokenizer = new StringTokenizer(record, DEFAULT_DELIMITER);
		}
		
		MovieRating movie = new MovieRating();
		boolean isValid = true;
		
		try {
			if((isValid = tokenizer.hasMoreTokens())) {
				movie.setUserId(Long.parseLong(tokenizer.nextToken()));
			}
			
			if((isValid = tokenizer.hasMoreTokens())) {
				movie.setItemId(Long.parseLong(tokenizer.nextToken()));
			}
			
			if((isValid = tokenizer.hasMoreTokens())) {
				movie.setRating(Integer.parseInt(tokenizer.nextToken()));
			}
			
			if((isValid = tokenizer.hasMoreTokens())) {
				movie.setTimestamp(Long.parseLong(tokenizer.nextToken()));
			}
			
			isValid = !tokenizer.hasMoreTokens();
			
		} catch(NumberFormatException e) {
			isValid = false;
		}
		
		return (isValid) ? movie : null;
	}
	
	public MovieRating pars(Text rawRecord, String delimiter) {
		return pars(rawRecord.toString(), delimiter);
	}
	
}
