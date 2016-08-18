/*
Description:
	Use a custom Partitioner to improve grouping
	The Partitioner assigns each record to a partition
	according to the following rule:
	partition = rating % gradeOfParallelism

Run:
	pig -param_file ../paths partitioner.pig
*/
REGISTER pig-helper-1.0.0-SNAPSHOT.jar;

data_rating = LOAD '$PATH_DATA/data' AS (userId:long, itemId:long, rating:int, timestamp:long);
data_grouped = GROUP data_rating BY rating PARTITION BY com.bigdata.pighelper.partitioner.CustomPartitioner PARALLEL 3;
STORE data_grouped INTO '$PATH_PARTITIONER';
