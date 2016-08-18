package com.bigdata.mapreduce.mapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bigdata.mapreduce.JobFactory;
import com.bigdata.mapreduce.parser.MovieParser;
import com.bigdata.mapreduce.parser.MovieRating;

public class AvroMapper extends Mapper<LongWritable, Text,
	AvroKey<Long>, AvroValue<GenericRecord>>{

	private static final Log LOG = LogFactory.getLog(AvroMapper.class);
			
	private HashMap<Long, User> userMap;
	private MovieParser movieParser;
	private MovieRating movieRating;
	
	// Avro related
	private GenericRecord ratingRecord;
	private GenericRecord userRecord;
	private Parser avroParser;
	private Schema ratingSchema;
	private Schema userSchema;
	
	@Override
	public void setup(Context context) throws IOException {
		
		avroParser = new Schema.Parser();
		
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI("hdfs://localhost:9000"), context.getConfiguration());
			
			// Get InputStream for User schema
			Path pathUser = new Path(JobFactory.AVRO_BASE + JobFactory.AVRO_USER);
	        FileStatus[] statusUser = fs.listStatus(pathUser);
	        userSchema = avroParser.parse(fs.open(statusUser[0].getPath()));
			
			// Get InputStream for Rating schema
			Path pathRating = new Path(JobFactory.AVRO_BASE + JobFactory.AVRO_RATING);
			FileStatus[] statusRating = fs.listStatus(pathRating);
			ratingSchema = avroParser.parse(fs.open(statusRating[0].getPath()));
			
			userRecord = new GenericData.Record(userSchema);
			ratingRecord = new GenericData.Record(ratingSchema);
			
			userMap = new HashMap<Long, User>();
			readUserInfo(JobFactory.HDFS_BASE + JobFactory.INPUT_USER);
			
			movieParser = new MovieParser();
		} catch (URISyntaxException e) {
			LOG.fatal("Could not read schema due to URISyntaxException");
			e.printStackTrace();
		}
		
		
	}
	
	
	@Override
	protected void map(LongWritable writableKey, Text value, Context context) throws IOException, InterruptedException {
		
		movieRating = movieParser.pars(value, null);
		long userKey = movieRating.getUserId();
		User user = userMap.get(userKey);
		
		if(movieRating != null && user != null) {
			userRecord.put("userId", userKey);
			userRecord.put("age", user.getAge());
			userRecord.put("gender", user.getGender());
			userRecord.put("occupation", user.getOccupation());
			userRecord.put("zip", user.getZip());
			
			ratingRecord.put("user", userRecord);
			ratingRecord.put("itemId", movieRating.getItemId());
			ratingRecord.put("rating", movieRating.getRating());
			ratingRecord.put("timestamp", movieRating.getTimestamp());
			
			context.write(new AvroKey<Long>(movieRating.getItemId()),
					new AvroValue<GenericRecord>(ratingRecord));
		}
	}
	
	
	private void readUserInfo(String fileName) throws FileNotFoundException {

	    if(fileName != null) {
			try{
				Path path = new Path(fileName);
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
                FileStatus[] status = fs.listStatus(path);
                
                String[] user;
                String line;
                BufferedReader reader;
                long lineNumber;
                
                for (int i = 0; i < status.length; i++){
                    reader = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                    
                    lineNumber = 1;
                    line = reader.readLine();
                    
                    while (line != null){
                    	try {
                    		// userId | age | gender | occupation | zip
            				user = line.split("\\|");
            				userMap.put(
            						Long.parseLong(user[0]),
            						new User(Integer.parseInt(user[1]), user[2], user[3], user[4]));
        				} catch(Exception e) {
        					LOG.error("Could not parse line no " + lineNumber + " in file '" + status[i].getPath().getName() + "'");
        				}
        				line = reader.readLine();
        				lineNumber++;
                    }
                    reader.close();
                }
                    
	        } catch(Exception e){
                e.printStackTrace();
	        }
		} else {
			LOG.fatal("File '" + fileName + "' could not be found!");
		}
	}

	private class User {
		int age;
		String gender;
		String occupation;
		String zip;
		
		public User(int age, String gender, String occupation, String zip) {
			super();
			this.age = age;
			this.gender = gender;
			this.occupation = occupation;
			this.zip = zip;
		}

		public int getAge() {
			return age;
		}

		public String getGender() {
			return gender;
		}

		public String getOccupation() {
			return occupation;
		}

		public String getZip() {
			return zip;
		}
	}
}
