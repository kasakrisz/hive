--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=false;
set hive.optimize.topnkey=true;

set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.tez.min.bloom.filter.entries=1;

set hive.tez.dynamic.partition.pruning=true;
set hive.stats.fetch.column.stats=true;
set hive.cbo.enable=true;


drop table if exists tstore;

create table t_test(
  a int,
  b int,
  c int
);

insert into t_test values
(5, 2, 3),
(6, 2, 1),
(7, 8, 4),(7, 8, 4),(7, 8, 4),
(5, 1, 2), (5, 1, 2), (5, 1, 2);


explain
select a, count(b) from
  (select a, b from t_test group by a, b) t1
group by a order by a limit 2;
select a, count(b) from
  (select a, b from t_test group by a, b) t1
group by a order by a limit 2;


set hive.optimize.topnkey=false;


explain
select a, count(b) from
(select a, b from t_test group by a, b) t1
group by a order by a limit 2;
select a, count(b) from
(select a, b from t_test group by a, b) t1
group by a order by a limit 2;
