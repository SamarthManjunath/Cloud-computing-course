/*
Name: Samarth Manjunath
UTA ID: 1001522809
Subject: Advanced Database Systems
Assignment-6
*/

input_entity = LOAD '$G' USING PigStorage(',') AS (ID:long, link:long);

group_entity = GROUP input_entity by link;

group_by_entity = FOREACH group_entity GENERATE group, COUNT(input_entity);

output_entity = ORDER group_by_entity BY $1 DESC;

STORE output_entity INTO '$O' USING PigStorage(',');