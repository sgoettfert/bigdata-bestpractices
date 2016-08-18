Big Data Best Practices - Example Project
===

Installation prerequisites
-----

Make sure the following software is installed (including the setting of the respective classpath):

* Java
* Scala
* Python 3
* HDFS
* Apache Hadoop
* Apache Pig
* Apache Spark
* Apache Hive
* Apache Sqoop
* MySQL

Configuration prerequisites
-----

Before running the project you have to prepare your environment

* Start a MySQL DB and create a database and a user both named 'bigdata'. The user's password is 'bigdata' as well and should have all rights and the database with the same name.

and download the necessary data sets:

* Download the MovieLens 100K Dataset (http://grouplens.org/datasets/movielens/100k/), unpack the archive and copy the file 'u.user' into the project's folder 'data'
* Download the latest MovieLens Datasets (http://grouplens.org/datasets/movielens/latest/), unpack the archive and copy the files 'movies.csv' and 'ratings.csv' into the projects's folder 'data'

Run the project
-----

Now you should be able to run the example project. Open a Bash, cd into the project root and type:
	$ ./exec_project.sh
