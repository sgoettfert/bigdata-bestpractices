#!/bin/bash
echo '*** Executing script "1_1_sampling.pig"'
pig -param_file paths 1_1_sampling.pig

echo ''
echo ''
echo '*** Executing script "1_2_typeinference.pig"'
pig -param_file paths 1_2_typeinference.pig

echo ''
echo ''
echo '*** Executing script "1_3_not_null.pig"'
pig -param_file paths 1_3_not_null.pig

echo ''
echo ''
echo '*** Executing script "1_4_types.pig"'
pig -param_file paths 1_4_types.pig

echo ''
echo ''
echo '*** Executing script "1_5_regex.pig"'
pig -param_file paths 1_5_regex.pig
