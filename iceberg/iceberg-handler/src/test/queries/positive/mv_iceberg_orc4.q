-- MV source tables are iceberg tables but MV is not
-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists tbl_ice;

create external table tbl_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='1');

insert into tbl_ice values (1, 'one v2', 50), (4, 'four v2', 53), (5, 'five v2', 54);

create materialized view mat1  as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

create materialized view mat2  as
select tbl_ice.b, tbl_ice.c, sum(tbl_ice.c) from tbl_ice where tbl_ice.c > 52 group by tbl_ice.b, tbl_ice.c;

create materialized view mat3  as
select tbl_ice.b, tbl_ice.c, sum(tbl_ice.c), avg(b) from tbl_ice where tbl_ice.c > 52 group by tbl_ice.b, tbl_ice.c;

-- insert some new values to one of the source tables
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54);

--set hive.materializedview.union.rewriter=false;

explain cbo
alter materialized view mat1 rebuild;

explain cbo
alter materialized view mat2 rebuild;

explain cbo
alter materialized view mat3 rebuild;
