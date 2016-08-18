package com.bigdata.mapreduce.parser;

public class MovieRating {
	long userId;
	long itemId;
	int rating;
	long timestamp;
	
	public MovieRating() {
		userId = 0;
		itemId = 0;
		rating = 0;
		timestamp = 0;
	}
	
	public MovieRating(long userId, long itemId, int rating, long timestamp) {
		super();
		this.userId = userId;
		this.itemId = itemId;
		this.rating = rating;
		this.timestamp = timestamp;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public long getItemId() {
		return itemId;
	}

	public void setItemId(long itemId) {
		this.itemId = itemId;
	}

	public int getRating() {
		return rating;
	}

	public void setRating(int rating) {
		this.rating = rating;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public long[] getArrayByUser() {
		long[] array = new long[3];
		array[0] = itemId;
		array[1] = rating;
		array[2] = timestamp;
		
		return array;
	}
	
	public long[] getArrayByMovie() {
		long[] array = new long[3];
		array[0] = userId;
		array[1] = rating;
		array[2] = timestamp;
		
		return array;
	}

}
