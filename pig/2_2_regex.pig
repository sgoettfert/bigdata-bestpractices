-- process ratings
data_rating = LOAD '$PATH_2_1_NULL/rating' AS (userId:chararray, itemId:chararray, rating:chararray, timestamp:chararray);

-- filter out invalid records
valid_rating = FILTER data_rating BY (userId matches '\\d+' AND itemId matches '\\d+' AND rating matches '\\d{1}\\.\\d{1}');
-- cast the rating to integer for further processing
valid_rating_casted = FOREACH valid_rating GENERATE userId, itemId, (int) rating, timestamp;
STORE valid_rating_casted INTO '$PATH_2_2_REGEX/rating';

-- process movies
data_item = LOAD '$PATH_2_1_NULL/item' AS (movieId:chararray, title:chararray, genres:chararray);

-- filter out invalid records
valid_item = FILTER data_item BY (movieId matches '\\d+');

STORE valid_item INTO '$PATH_2_2_REGEX/item';
