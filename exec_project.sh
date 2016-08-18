#!/bin/bash

# Base path on HDFS
# Notice: If you like to modify this you have to
# adapt the path in the following files as well:
# 	-'Pig/paths'
# 	-'Hive/hive.sql'
MY_HDFS_BASE=/user/bigdata

# Make sure that a MySQL DB exists on the specified host
# containing the respective database and a user with
# respective credentials (and rights on that DB, of course..)
MY_DB=jdbc:mysql://localhost/bigdata
MY_DB_USER=bigdata
MY_DB_PASS=bigdata

# Set up environment on HDFS
cd data
. data_setup.sh

# Import from MySQL DB with Sqoop
echo "Running Sqoop to import data from MySQL DB.."
cd ../sqoop
. exec_sqoop_import.sh

# OPTIONAL
# Uglify rating records
cd ../data
. uglify.sh 

# Launch Pig scripts for Data Understanding
echo "Running Pig scripts for Data Understanding.."
cd ../pig
. exec_pig_understanding.sh

# Launch HiveQL scripts for Data Exploration
echo "Running HiveQL scripts for Data Understanding.."
cd ../hive
. exec_hive.sh

# Launch Pig scripts for Data Preparation
echo "Running Pig scripts for Data Preparation.."
cd ../pig
. exec_pig_preparation.sh

# Launch MapReduce job
echo "Running MapReduce job.."
cd ../mapreduce
hadoop jar target/mapreduce-1.0.0-SNAPSHOT.jar com.bigdata.mapreduce.App inverted hdfs://localhost:9000/$MY_HDFS_BASE/result_2_preparation/5_titles

# Launch Spark job
echo "Running Spark job.."
cd ../spark
spark-submit --class com.bigdata.scala.App --master spark://ubuntu-VirtualBox:7077 target/spark-1.0.0-SNAPSHOT.jar inverted hdfs://localhost:9000/$MY_HDFS_BASE/result_2_preparation/5_titles

# Export to MySQL DB with Sqoop
echo "Running Sqoop to export result data to MySQL DB.."
cd ../sqoop
. exec_sqoop_export.sh

echo ""
echo "Finished!"
