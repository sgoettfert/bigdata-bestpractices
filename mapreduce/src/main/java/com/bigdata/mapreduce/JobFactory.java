package com.bigdata.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import com.bigdata.mapreduce.combiner.AVGRatingCombiner;
import com.bigdata.mapreduce.mapper.AVGRatingMapper;
import com.bigdata.mapreduce.mapper.AvroMapper;
import com.bigdata.mapreduce.mapper.BinningMapper;
import com.bigdata.mapreduce.mapper.ByMovieMapper;
import com.bigdata.mapreduce.mapper.ByUserMapper;
import com.bigdata.mapreduce.mapper.FilterMapper;
import com.bigdata.mapreduce.mapper.JoinMapper;
import com.bigdata.mapreduce.mapper.ProjectionMapper;
import com.bigdata.mapreduce.mapper.ToTextMapper;
import com.bigdata.mapreduce.partitioner.RatingPartitioner;
import com.bigdata.mapreduce.reducer.AVGRatingReducer;
import com.bigdata.mapreduce.reducer.AvroReducer;
import com.bigdata.mapreduce.reducer.InvertedIndexReducer;
import com.bigdata.mapreduce.reducer.SortReducer;
import com.bigdata.mapreduce.writable.LongArrayWritable;
import com.bigdata.mapreduce.writable.LongPairWritable;

public class JobFactory {

	public static final String HDFS_BASE = "hdfs://localhost:9000/user/bigdata/";
	
	public static final String INPUT_DATA = "result_2_preparation/4_duplicates/rating";
	public static final String INPUT_USER = "data/user";
	
	public static final String RESULT = "result_3/mapreduce";
	public static final String OUTPUT = "output_mapreduce_jobs/";
	public static final String OUTPUT_INTER = "output_inter";
	
	public static final String AVRO_BASE = HDFS_BASE + "data/avro/";
	public static final String AVRO_RATING = "Rating.avsc";
	public static final String AVRO_USER = "User.avsc";
	
	public JobFactory() {
		super();
	}
	
	public Job getAvroJob(Configuration conf) throws IOException, URISyntaxException {
		Configuration readConf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), readConf);
		
		Parser parser = new Schema.Parser();

		// Get InputStream for User schema
		Path pathUser = new Path(AVRO_BASE + AVRO_USER);
        FileStatus[] statusUser = fs.listStatus(pathUser);
		parser.parse(fs.open(statusUser[0].getPath()));
		
		// Get InputStream for Rating schema
		Path pathRating = new Path(AVRO_BASE + AVRO_RATING);
		FileStatus[] statusRating = fs.listStatus(pathRating);
		Schema SCHEMA = parser.parse(fs.open(statusRating[0].getPath()));
		
		Job job = Job.getInstance(conf, "Avro Job");
		
	    job.getConfiguration().setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

	    FileInputFormat.setInputPaths(job, new Path(HDFS_BASE + INPUT_DATA));
	    FileOutputFormat.setOutputPath(job, new Path(HDFS_BASE + OUTPUT + "avro"));

	    AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.LONG));
	    AvroJob.setMapOutputValueSchema(job, SCHEMA);
	    
	    job.setMapperClass(AvroMapper.class);
	    job.setReducerClass(AvroReducer.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    
		return job;
	}
	
	public Job getAVGRatingJob(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, "AVG Rating");
		job.setJarByClass(App.class);
		
		job.setMapperClass(AVGRatingMapper.class);
		job.setCombinerClass(AVGRatingCombiner.class);
		job.setReducerClass(AVGRatingReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(HDFS_BASE + INPUT_DATA));
		FileOutputFormat.setOutputPath(job, new Path(HDFS_BASE + OUTPUT + "average"));
		
		return job;
	}
	
	public Job getBinningJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "Binning");
		job.setJarByClass(App.class);
		
		job.setMapperClass(BinningMapper.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(HDFS_BASE + INPUT_USER));
		FileOutputFormat.setOutputPath(job, new Path(HDFS_BASE + OUTPUT + "binning"));
		
		MultipleOutputs.addNamedOutput(job, "occupation", TextOutputFormat.class, LongWritable.class, Text.class);
		
		return job;
	}
	
	public Job getChainedJob(String minDate, Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, "By Movie Mapping");
		job.setJarByClass(App.class);
		
		Configuration mapperConf = new Configuration(false);	
		Configuration filterConf = new Configuration(false);
		filterConf.setInt("FILTER_POSITION", 2);
		filterConf.setLong("FILTER_MIN_VAL", Long.parseLong(minDate));
		
		ChainMapper.addMapper(job, ByMovieMapper.class, LongWritable.class, Text.class,
				LongWritable.class, LongArrayWritable.class, mapperConf);
		ChainMapper.addMapper(job, FilterMapper.class, LongWritable.class, LongArrayWritable.class,
				LongWritable.class, LongArrayWritable.class, filterConf);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(HDFS_BASE + INPUT_DATA));
		FileOutputFormat.setOutputPath(job, new Path(HDFS_BASE + OUTPUT + "chained"));
		
		return job;
	}
	
	public Job getInvertedIndexJob(Configuration conf, String titlesFile) throws IOException {
		Job job = Job.getInstance(conf, "Inverted Index");
		job.setJarByClass(App.class);
		
		Configuration mapperConf = new Configuration(false);	
		Configuration projectionConf = new Configuration(false);
		projectionConf.setInt("PROJECTION_POSITION", 0);
		
		ChainMapper.addMapper(job, ByMovieMapper.class, LongWritable.class, Text.class,
				LongWritable.class, LongArrayWritable.class, mapperConf);
		ChainMapper.addMapper(job, ProjectionMapper.class, LongWritable.class, LongArrayWritable.class,
				LongWritable.class, LongArrayWritable.class, projectionConf);
		
		job.setCombinerClass(InvertedIndexReducer.class);
		
		Configuration reduceConf = new Configuration(false);
		Configuration joinConf = new Configuration(false);
		joinConf.set(JoinMapper.TITLES_FILE, titlesFile);
		
		ChainReducer.setReducer(job, InvertedIndexReducer.class, LongWritable.class, LongArrayWritable.class,
				LongWritable.class, LongArrayWritable.class, reduceConf);
		ChainReducer.addMapper(job, JoinMapper.class, LongWritable.class, LongArrayWritable.class,
				Text.class, LongArrayWritable.class, joinConf);
		
		FileInputFormat.addInputPath(job, new Path(HDFS_BASE + INPUT_DATA));
		FileOutputFormat.setOutputPath(job, new Path(HDFS_BASE + RESULT));
		
		return job;
	}
	
	public Job getPartitiongJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "Partitioning");
		job.setJarByClass(App.class);
		
		job.setMapperClass(ByUserMapper.class);
		job.setPartitionerClass(RatingPartitioner.class);
		
		// ratings range from 1 to 5
		job.setNumReduceTasks(5);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongArrayWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(HDFS_BASE + INPUT_DATA));
		FileOutputFormat.setOutputPath(job, new Path(HDFS_BASE + OUTPUT + "partitioning"));
		
		return job;
	}
	
	public Job getSortingPrepJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "Total Order Sort - Preparation");
		job.setJarByClass(App.class);

		Configuration mapperConf = new Configuration(false);	
		Configuration projectionConf = new Configuration(false);
		Configuration textConf = new Configuration(false);
		projectionConf.setInt("PROJECTION_POSITION", 0);
		
		ChainMapper.addMapper(job, ByMovieMapper.class, LongWritable.class, Text.class,
				LongWritable.class, LongArrayWritable.class, mapperConf);
		ChainMapper.addMapper(job, ProjectionMapper.class, LongWritable.class, LongArrayWritable.class,
				LongWritable.class, LongArrayWritable.class, projectionConf);
		ChainMapper.addMapper(job, ToTextMapper.class, LongWritable.class, LongArrayWritable.class,
				LongWritable.class, Text.class, textConf);
		
		job.setCombinerClass(SortReducer.class);
	    job.setNumReduceTasks(0);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    TextInputFormat.setInputPaths(job, new Path(HDFS_BASE + INPUT_DATA));

	    // Set the output format to a sequence file.
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    SequenceFileOutputFormat.setOutputPath(job, new Path(HDFS_BASE + OUTPUT_INTER));
	    
	    return job;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Job getSortingJob(Configuration conf, int numReducers, double sampleRate) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "Total Order Sorting");
		job.setJarByClass(App.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(HDFS_BASE + OUTPUT_INTER));
		
		TextOutputFormat.setOutputPath(job, new Path(HDFS_BASE + OUTPUT + "sorting"));

		job.setMapperClass(Mapper.class);
		job.setReducerClass(SortReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(numReducers);

		// Use Total Order Partitioner.
		job.setPartitionerClass(TotalOrderPartitioner.class);

	    // Generate partition file from map-only job's output.
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(HDFS_BASE + "partition"));
	    InputSampler.writePartitionFile(job, new InputSampler.RandomSampler(sampleRate, numReducers));

	    return job;
	}
}
