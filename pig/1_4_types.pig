REGISTER udfs/datatypeinfererudf.jar;
REGISTER udfs/customdatatypeinfererstorage.jar;

-- macro to count the matching and non matching records for given type
DEFINE get_types(raw, column, type) RETURNS result {
	data = FOREACH $raw GENERATE com.profiler.DataTypeInfererUDF($column) AS inferred;
	SPLIT data INTO valid IF (inferred == '$type'),
		invalid OTHERWISE;
	grouped_valid = GROUP valid ALL;
	count_valid = FOREACH grouped_valid GENERATE '$column', 'valid', LOWER('$type'), COUNT(valid);
	grouped_invalid = GROUP invalid ALL;
	count_invalid = FOREACH grouped_invalid GENERATE '$column', 'invalid', LOWER('$type'), COUNT(invalid);
	$result = UNION count_valid, count_invalid;
}

-- explore files containing ratings
data_rating = LOAD '$PATH_DATA/data' AS (userId:chararray, itemId:chararray, rating:chararray, timestamp:chararray);
types_rating_userId = get_types(data_rating, 'userId', 'int');
types_rating_itemId = get_types(data_rating, 'itemId', 'int');
types_rating_rating = get_types(data_rating, 'rating', 'int');
types_rating_timestamp = get_types(data_rating, 'timestamp', 'int');
result_rating = UNION types_rating_userId, types_rating_itemId, types_rating_rating, types_rating_timestamp;
result_rating_ordered = ORDER result_rating BY $0;
STORE result_rating_ordered INTO '$PATH_1_4_TYPE/rating';

-- explore files containing users
data_user = LOAD '$PATH_DATA/user' USING PigStorage('|') AS (userId:chararray, age:chararray, gender:chararray, occupation:chararray, zip:chararray);
types_user_user = get_types(data_user, 'userId', 'int');
types_user_age = get_types(data_user, 'age', 'int');
types_user_gender = get_types(data_user, 'gender', 'chararray');
types_user_occupation = get_types(data_user, 'occupation', 'chararray');
types_user_zip = get_types(data_user, 'zip', 'int');
result_user = UNION types_user_user, types_user_age, types_user_gender, types_user_occupation, types_user_zip;
result_user_ordered = ORDER result_user BY $0;
STORE result_user_ordered INTO '$PATH_1_4_TYPE/user';

-- explore files containing movies
data_item = LOAD '$PATH_DATA/item' USING PigStorage(',') AS (movieId:chararray, title:chararray, genres:chararray);
types_item_movieId = get_types(data_item, 'movieId', 'int');
types_item_title = get_types(data_item, 'title', 'chararray');
types_item_genres = get_types(data_item, 'genres', 'chararray');
result_item = UNION types_item_movieId, types_item_title, types_item_genres;
result_item_ordered = ORDER result_item BY $0;
STORE result_item_ordered INTO '$PATH_1_4_TYPE/item';
