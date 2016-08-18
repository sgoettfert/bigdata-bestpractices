#!/bin/bash

### Prepare files
echo "Removing header from files"

tail -n +2 movies.csv > movies.tmp
mv movies.tmp movies.csv

tail -n +2 ratings.csv > ratings.tmp
mv ratings.tmp ratings.csv

### Prepare HDFS
echo "Setting up HDFS environment.."

# build json file from data
python3 json.py

# Create directories
hdfs dfs -mkdir -p $MY_HDFS_BASE/data
hdfs dfs -mkdir $MY_HDFS_BASE/data/item
hdfs dfs -mkdir $MY_HDFS_BASE/data/user
hdfs dfs -mkdir $MY_HDFS_BASE/data/avro
hdfs dfs -mkdir $MY_HDFS_BASE/data/json

# Upload data from local file system
hdfs dfs -put movies.csv $MY_HDFS_BASE/data/item/movies.csv
hdfs dfs -put u.user $MY_HDFS_BASE/data/user/u.user
hdfs dfs -put avro/Rating.avsc $MY_HDFS_BASE/data/avro/Rating.avsc
hdfs dfs -put avro/User.avsc $MY_HDFS_BASE/data/avro/User.avsc
hdfs dfs -put data.json $MY_HDFS_BASE/data/json/data.json
echo ""

### Prepare MySQL DB
echo "Setting up DB environment.."
java -jar mysql-filler.jar $MY_DB $MY_DB_USER $MY_DB_PASS ratings.csv
echo ""
