#!/bin/bash

# Create a password file and unset password variable
touch pass.txt
echo -n $MY_DB_PASS > pass.txt
chmod 400 pass.txt
MY_DB_PASS=""

# Import data from DB into HDFS
sqoop import --connect $MY_DB --table rating --username $MY_DB_USER --password-file file://$PWD/pass.txt --fields-terminated-by \\t --target-dir $MY_HDFS_BASE/data/data
