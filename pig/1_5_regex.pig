-- macro to count and store the number of valid & invalid records
DEFINE get_summary(valid, invalid, name) RETURNS void {
	val_grpd = GROUP $valid ALL;
	val_count = FOREACH val_grpd GENERATE 'valid', COUNT($valid);
	inv_grpd = GROUP $invalid ALL;
	inv_count = FOREACH inv_grpd GENERATE 'invalid', COUNT($invalid);
	summary = UNION val_count, inv_count;
	ordered_summary = ORDER summary BY $0;
	STORE ordered_summary INTO '$PATH_1_5_REGEX/$name';
}

-- explore files containing ratings
data_rating = LOAD '$PATH_DATA/data' AS (userId:chararray, itemId:chararray, rating:chararray, timestamp:chararray);
SPLIT data_rating INTO valid_data IF (userId matches '\\d+') AND (itemId matches '\\d+') AND
	(rating matches '\\d{1}\\.\\d{1}') AND (timestamp matches '\\d+'),
	invalid_data OTHERWISE;
get_summary(valid_data, invalid_data, 'rating');

-- explore files containing movies
data_item = LOAD '$PATH_DATA/item' USING PigStorage(',') AS (movieId:chararray, title:chararray, genres:chararray);
SPLIT data_item INTO valid_data IF
	(movieId matches '\\d+') AND (title is not null),
	invalid_data OTHERWISE;
get_summary(valid_data, invalid_data, 'item');

-- explore files containing users
data_user = LOAD '$PATH_DATA/user' USING PigStorage('|') AS (userId:chararray, age:chararray, gender:chararray, occupation:chararray, zip:chararray);
SPLIT data_user INTO valid_user IF (userId matches '\\d+') AND (age matches '\\d{1,3}') AND
	(gender matches '[mMfF]') AND (occupation matches '[a-zA-Z]+') AND (zip matches '\\w+'),
	invalid_user OTHERWISE;
get_summary(valid_user, invalid_user, 'user');

