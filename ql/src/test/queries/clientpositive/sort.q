--! qt:dataset:src
-- SORT_QUERY_RESULTS
set hive.cbo.enable=true;

CREATE TABLE src_x1(key string, value string);
CREATE TABLE src_x2(key string, value string);

explain
from src a join src b on a.key = b.key
insert overwrite table src_x1
select a.key,"" sort by a.key
insert overwrite table src_x2
select a.value,"" sort by a.value;