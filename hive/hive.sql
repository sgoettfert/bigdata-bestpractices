-- Configure base path
set my_base_path=/user/bigdata;

-- Create tables
CREATE EXTERNAL TABLE IF NOT EXISTS items (movieid BIGINT, title STRING, genres STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '${hiveconf:my_base_path}/data/item';
CREATE EXTERNAL TABLE IF NOT EXISTS ratings (userid BIGINT, movieid BIGINT, rating FLOAT, time BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE location '${hiveconf:my_base_path}/data/data';
CREATE EXTERNAL TABLE IF NOT EXISTS users (userid BIGINT, age INT, gender STRING, occupation STRING, zip STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE location '${hiveconf:my_base_path}/data/user';

-- Perform queries on table 'items'
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/items_count' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT COUNT(*) FROM items;
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/items_count_unknown' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT COUNT(*) FROM items WHERE genres = '(no genres listed)';
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/items_count_comedy' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT COUNT(*) FROM items WHERE genres = 'comedy';

-- Perform queries on table 'ratings'
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/ratings_count' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT COUNT(*) FROM ratings;
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/movies_avg_rating' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT movieid, AVG(rating) FROM ratings GROUP BY movieId ORDER BY movieid;
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/movies_views_flop' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT movieid, COUNT(*) AS num_views FROM ratings GROUP BY movieId ORDER BY num_views ASC LIMIT 100;
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/movies_views_top' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT movieid, COUNT(*) AS num_views FROM ratings GROUP BY movieId ORDER BY num_views DESC LIMIT 100;
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/users_num_ratings' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT userid, COUNT(*) FROM ratings GROUP BY userid ORDER BY userid;
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/users_avg_rating' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT userid, AVG(rating) FROM ratings GROUP BY userid ORDER BY userid;

-- Perform queries on table 'users'
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/users_count' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT COUNT(*) FROM users;
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/users_age' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT age, COUNT(*) FROM users GROUP BY age ORDER BY age ASC;
INSERT OVERWRITE DIRECTORY '${hiveconf:my_base_path}/result_1_understanding/6_hive/users_gender' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE SELECT gender, COUNT(*) FROM users GROUP BY gender;
