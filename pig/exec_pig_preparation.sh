#!/bin/bash
echo '*** Executing script "2_1_nulls.pig"'
pig -param_file paths 2_1_nulls.pig

echo ''
echo ''
echo '*** Executing script "2_2_regex.pig"'
pig -param_file paths 2_2_regex.pig

echo ''
echo ''
echo '*** Executing script "2_3_range.pig"'
pig -param_file paths 2_3_range.pig

echo ''
echo ''
echo '*** Executing script "2_4_duplicates.pig"'
pig -param_file paths 2_4_duplicates.pig

echo ''
echo ''
echo '*** Executing script "2_5_titles.pig"'
pig -param_file paths 2_5_titles.pig
