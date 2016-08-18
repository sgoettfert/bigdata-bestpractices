-- generate and store the projection of 'ID' and 'title'
data_item = LOAD '$PATH_2_4_DUPLICATES/item' AS (movieId:long, title:chararray, genres:chararray);
data_item_titles = FOREACH data_item GENERATE movieId, title;
STORE data_item_titles INTO '$PATH_2_5_TITLES';
