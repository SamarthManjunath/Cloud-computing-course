--Name: Samarth Manjunath
--UTA ID: 1001522809
-- Subject: Advanced Database Systems
drop table sample_graph;

create table sample_graph(
source_entity BIGINT,
destination_entity BIGINT)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table sample_graph;

INSERT OVERWRITE TABLE sample_graph
SELECT destination_entity,count(*)
FROM sample_graph
GROUP BY destination_entity;

SELECT source_entity,destination_entity
FROM sample_graph
SORT BY destination_entity desc;