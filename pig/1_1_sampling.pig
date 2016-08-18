%default MAX_NUMBER 50;

REGISTER udfs/datafu.jar;

DEFINE SRS datafu.pig.sampling.SimpleRandomSample('0.25');

-- macro to draw a sample from a passed relation
DEFINE draw_sample(raw) RETURNS result {
	grouped = GROUP $raw ALL;
	sampled = FOREACH grouped GENERATE FLATTEN(SRS($raw));
	$result = LIMIT sampled $MAX_NUMBER;
}

data_rating = LOAD '$PATH_DATA/data';
data_rating_samp = draw_sample(data_rating);
STORE data_rating_samp INTO '$PATH_1_1_SAMPLE/rating';

data_item = LOAD '$PATH_DATA/item' USING PigStorage(',');
data_item_samp = draw_sample(data_item);
STORE data_item_samp INTO '$PATH_1_1_SAMPLE/item' USING PigStorage(',');

data_user = LOAD '$PATH_DATA/user' USING PigStorage('|');
data_user_samp = draw_sample(data_user);
STORE data_user_samp INTO '$PATH_1_1_SAMPLE/user' USING PigStorage('|');
