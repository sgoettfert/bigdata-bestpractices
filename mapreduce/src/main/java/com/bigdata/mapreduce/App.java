package com.bigdata.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class App extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(App.class);
	private static final JobFactory FACTORY = new JobFactory();
	
    public static void main( String[] args ) throws Exception {
    	App driver = new App();
		int exitCode = ToolRunner.run(new Configuration(), driver, args);
		LOG.info("Finished Job with Exit Code: " + exitCode);
		System.exit(exitCode);
    }
    
    public int run(String[] args) throws Exception {
    	Job job = null;
    	
    	if(0 < args.length) {
    		try {
    			// determine which job to run
	    		if(args[0].equals("avro")) {
	    			job = FACTORY.getAvroJob(getConf());
	    			
	    		} else if(args[0].equals("average")) {
	    			job = FACTORY.getAVGRatingJob(getConf());
	    		
	    		} else if(args[0].equals("binning")) {
	    			job = FACTORY.getBinningJob(getConf());
	    			
	    		} else if(args[0].equals("chaining")) {
	    			job = FACTORY.getChainedJob(args[1], getConf());
	    			
	    		} else if(args[0].equals("inverted")) {
	    			String titlesFile = args[1];
	    			job = FACTORY.getInvertedIndexJob(getConf(), titlesFile);
	    			
	    		} else if(args[0].equals("partition")) {
	    			job = FACTORY.getPartitiongJob(getConf());
	    			
	    		} else if(args[0].equals("sorting")) {
	    			// execute a preparation job prior to actual sorting
	    			Job interJob = FACTORY.getSortingPrepJob(getConf());
	    			int prepStatus = interJob.waitForCompletion(true) ? 0: 1;
	    			
	    			if(prepStatus == 0) {
	    				// now get job for actual sorting
	    				job = FACTORY.getSortingJob(new Configuration(), Integer.parseInt(args[1]), Double.parseDouble(args[2]));
	    			} else {
	    				throw new Exception("Error in preparation for Total Order Sorting");
	    			}
	    		} else {
	    			LOG.info("No suitable parameter passed for deciding action!");
	        		LOG.info("Try: average | avro | binning | chaining | inverted | partition | sorting");
	        		return 1;
	    		}

	    		return job.waitForCompletion(true) ? 0 : 1;
	    		
    		} catch(ArrayIndexOutOfBoundsException e) {
    			LOG.error("Missing parameter(s)!");
    			return 1;
    		} catch(Exception e) {
    			LOG.error("Unexpexted error occured: " + e.getMessage());
    			return 2;
    		}
    	} else {
    		
    		LOG.info("No parameter passed for deciding action!");
    		LOG.info("Try: average | avro | binning | chaining | inverted | partition | sorting");
    		return 1;
    	}		
	}
}
