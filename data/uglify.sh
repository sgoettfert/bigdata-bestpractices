#!/bin/bash

echo "Uglifying data.."

# create uglified data
python3 uglify.py

# remove files on HDFS
hdfs dfs -rm -r $MY_HDFS_BASE/data/data
hdfs dfs -rm -r $MY_HDFS_BASE/data/item

# put uglified data records on HDFS
hdfs dfs -mkdir $MY_HDFS_BASE/data/data
hdfs dfs -mkdir $MY_HDFS_BASE/data/item
hdfs dfs -put uglified/ratings.csv $MY_HDFS_BASE/data/data/ratings.csv
hdfs dfs -put uglified/movies.csv $MY_HDFS_BASE/data/item/movies.csv
