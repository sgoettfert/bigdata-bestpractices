#!/bin/bash

# Export data from HDFS back into DB
sqoop export --connect $MY_DB --table inverted --username $MY_DB_USER --password-file file://$PWD/pass.txt --input-fields-terminated-by \\t --export-dir $MY_HDFS_BASE/result_3/mapreduce --driver com.mysql.jdbc.Driver
