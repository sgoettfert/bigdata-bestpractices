REGISTER udfs/datatypeinfererudf.jar;
REGISTER udfs/customdatatypeinfererstorage.jar;

-- macro for determining the types of a single column from a relation
DEFINE get_types(column, raw) RETURNS ordered {
	types = FOREACH $raw GENERATE com.profiler.DataTypeInfererUDF($column) AS inferred;
	grouped = GROUP types BY inferred;
	type_count = FOREACH grouped GENERATE '$column', group, COUNT(types) AS counts;
	$ordered = ORDER type_count BY counts DESC;
}

-- explore files containing rating information
raw_data = LOAD '$PATH_DATA/data' AS (user:chararray, item:chararray, rating:chararray, timestamp:chararray);

user = get_types('user', raw_data); -- check column 'user'
item = get_types('item', raw_data); -- check column 'item'
rating = get_types('rating', raw_data); -- check column 'rating'
timestamp = get_types('timestamp', raw_data); -- check column 'timestamp'

summary_rating = UNION user, item, rating, timestamp; -- combine results
ordered_summary_rating = ORDER summary_rating BY $0;
STORE ordered_summary_rating INTO '$PATH_1_2_TYPE/rating';

-- explore files containing user information
raw_user = LOAD '$PATH_DATA/user' USING PigStorage('|') AS (user:chararray, age:chararray, gender:chararray, occupation:chararray, zip:chararray);

userId = get_types('user', raw_user); -- check column 'user'
age = get_types('age', raw_user); -- check column 'age'
gender = get_types('gender', raw_user); -- check column 'gender'
occupation = get_types('occupation', raw_user); -- check column 'occupation'
zip = get_types('zip', raw_user); -- check column 'zip'

summary_user = UNION userId, age, gender, occupation, zip; -- combine results
ordered_summary_user = ORDER summary_user BY $0;
STORE ordered_summary_user INTO '$PATH_1_2_TYPE/user';

-- explore files containing movie information
raw_item = LOAD '$PATH_DATA/item' USING PigStorage(',') AS (movieId:chararray, title:chararray, genres:chararray);

movieId = get_types('movieId', raw_item);
title = get_types('title', raw_item);
genres = get_types('genres', raw_item);

summary_item = UNION movieId, title, genres;
ordered_summary_item = ORDER summary_item BY $0;
STORE ordered_summary_item INTO '$PATH_1_2_TYPE/item';

