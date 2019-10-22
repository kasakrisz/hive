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

create table tstore(
  a int,
  b int,
  c int,
  store_name varchar(100)
);

insert into tstore values
(5, 1, 2, 'cdf'), (5, 1, 2, 'tre'), (5, 1, 2, 'gtf'),
(5, 2, 3, 'abc'), (6, 2, 1, 'kfd');

--explain
--select a,b,c from tstore group by a,b,c order by a limit 2;
--select a,b,c from tstore group by a,b,c order by a limit 2;



--explain
--select a,b from tstore
--group by a,b order by a limit 2;
--select a,b from tstore
--group by a,b order by a limit 2;

--explain
--select a,b from
--  (select a,b,c from tstore group by a,b,c) sub
--group by a,b order by a limit 2;
--select a,b from
--  (select a,b,c from tstore group by a,b,c) sub
--group by a,b order by a limit 2;

explain
select a, count(b) from
  (select a, b from tstore group by a, b) t1
group by a order by a limit 2;
select a, count(b) from
  (select a, b from tstore group by a, b) t1
group by a order by a limit 2;


--explain
--select ctinyint, count(cdouble) from
--  (select ctinyint, cdouble from alltypesorc group by ctinyint, cdouble) t1
--group by ctinyint order by ctinyint limit 20;
--select ctinyint, count(cdouble) from
--  (select ctinyint, cdouble from alltypesorc group by ctinyint, cdouble) t1
--group by ctinyint order by ctinyint limit 20;

--set hive.optimize.topnkey=false;


--explain
--select a,b,c from tstore group by a,b,c order by a limit 2;
--select a,b,c from tstore group by a,b,c order by a limit 2;
--