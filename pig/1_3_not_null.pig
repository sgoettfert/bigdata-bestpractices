-- macro to count and store valid & invalid records
DEFINE get_summary(valid, invalid, name) returns void {
	val_grpd = GROUP $valid ALL;
	val_count = FOREACH val_grpd GENERATE 'valid', COUNT($valid);
	inv_grpd = GROUP $invalid ALL;
	inv_count = FOREACH inv_grpd GENERATE 'invalid', COUNT($invalid);
	summary = UNION val_count, inv_count;
	STORE summary INTO '$PATH_1_3_NOT_NULL/$name';
}

-- explore files containing ratings
data_rating = LOAD '$PATH_DATA/data' AS (userId:chararray, itemId:chararray, rating:chararray, timestamp:chararray);
SPLIT data_rating INTO valid_data IF (userId is not null) AND (itemId is not null) AND
	(rating is not null) AND (timestamp is not null),
	invalid_data OTHERWISE;
get_summary(valid_data, invalid_data, 'rating');

-- explore files containing users
data_user = LOAD '$PATH_DATA/user' USING PigStorage('|') AS (userId:chararray, age:chararray, gender:chararray, occupation:chararray, zip:chararray);
SPLIT data_user INTO valid_user IF (userId is not null) AND (age is not null) AND
	(gender is not null) AND (occupation is not null) AND (zip is not null),
	invalid_user OTHERWISE;
get_summary(valid_user, invalid_user, 'user');

-- explore files containing movies
data_item = LOAD '$PATH_DATA/item' USING PigStorage(',') AS (movieId:chararray, title:chararray, genres:chararray);
SPLIT data_item INTO valid_item IF (movieId is not null) AND (title is not null) AND (genres is not null),
	invalid_item OTHERWISE;
get_summary(valid_item, invalid_item, 'item');
