-- get unique records for ratings
data_rating = LOAD '$PATH_2_3_RANGE/rating' AS (userId:long, itemId:long, rating:int, timestamp:long);
data_rating_unique = DISTINCT data_rating;
STORE data_rating_unique INTO '$PATH_2_4_DUPLICATES/rating';

-- get unique records for movies
data_item = LOAD '$PATH_2_2_REGEX/item' AS (movieId:chararray, title:chararray, genres:chararray);
-- make sure, IDs are unique!
data_item_grouped = GROUP data_item BY movieId;
data_item_unique = FOREACH data_item_grouped {
    unique = TOP(1, 0, $1);
    GENERATE FLATTEN(unique);
}
STORE data_item_unique INTO '$PATH_2_4_DUPLICATES/item';
