-- extract and store the non-null rating records
data_rating = LOAD '$PATH_DATA/data' AS (userId:long, itemId:long, rating:float, timestamp:long);
SPLIT data_rating INTO
	valid IF (userId is not null) AND (itemId is not null) AND (rating is not null),
	invalid OTHERWISE;
STORE valid INTO '$PATH_2_1_NULL/rating';

-- extract and store the non-null movie records
data_item = LOAD '$PATH_DATA/item' USING PigStorage(',') AS (movieId:chararray, title:chararray, genres:chararray);
SPLIT data_item INTO
	valid IF (movieId is not null) AND (title is not null),
	invalid OTHERWISE;
STORE valid INTO '$PATH_2_1_NULL/item';
