package com.bigdata.mapreduce.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bigdata.mapreduce.writable.LongArrayWritable;

public class JoinMapper extends Mapper<LongWritable, LongArrayWritable, Text, LongArrayWritable> {
	
	private static final Log LOG = LogFactory.getLog(JoinMapper.class);

	public static final String TITLES_FILE = "JoinMapper-Titles";
	
	private HashMap<Long, String> titleMap;
	
	@Override
	public void setup(Context context) throws IOException {
		titleMap = new HashMap<Long, String>();
		
		String fileName = context.getConfiguration().get(TITLES_FILE);
		
		if(fileName != null) {
			try{
				Path path = new Path(fileName);
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
                FileStatus[] status = fs.listStatus(path);
                
                for (int i = 0; i < status.length; i++){
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                    String line;
                    long lineNumber = 1;
                    line = reader.readLine();
                    
                    while (line != null){
                    	try {
        					String[] pair = line.split("\t");
        					titleMap.put(Long.parseLong(pair[0]), pair[1]);
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
			LOG.fatal("No parameter TITLES_FILE was passed!");
		}
	}
	
	@Override
	public void map(LongWritable key, LongArrayWritable values, Context context)
			throws IOException, InterruptedException {
		
		String outKey = titleMap.get(key.get());
		
		if(outKey != null) {
			outKey = key.get() + "-" + outKey;
			context.write(new Text(outKey), values);
		}
	}
}
