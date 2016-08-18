-- split the records according to their value in field 'rating'
data_rating = LOAD '$PATH_2_2_REGEX/rating' AS (userId:long, itemId:long, rating:int, timestamp:long);
SPLIT data_rating INTO
	too_big IF (5 < rating),
	too_small IF (rating < 1),
	valid OTHERWISE;

-- adjust invalid value accordingly
too_big_cleaned = FOREACH too_big GENERATE userId, itemId, 5.0, timestamp;
too_small_cleaned = FOREACH too_small GENERATE userId, itemId, 1.0, timestamp;

-- merge cleaned and originally valid values before storing them alltogether
data_rating_cleaned = UNION valid, too_big_cleaned, too_small_cleaned;
STORE data_rating_cleaned INTO '$PATH_2_3_RANGE/rating';
