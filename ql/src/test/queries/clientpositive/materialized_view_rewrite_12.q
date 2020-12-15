set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting.sql=false;

create table t1(col0 int) stored as orc
                          tblproperties ('transactional'='true');

create materialized view mat_text tblproperties ('rewriting.scope'='TEXT') as
select * from t1 where col0 = 2;

explain cbo
select * from t1 where col0 = 2;

create materialized view mat_both tblproperties ('rewriting.scope'='Calcite, Text') as
select * from t1 where col0 = 2;

explain cbo
select * from t1 where col0 = 2;

drop materialized view mat_text;
drop materialized view mat_both;

-- MV can not be used for Calcite based rewrites since definition contains Union operator
create materialized view mat1 tblproperties ('rewriting.scope'='Calcite') as
select * from t1 where col0 = 1
union
select * from t1 where col0 = 2;

explain cbo
select * from t1 where col0 = 1
union
select * from t1 where col0 = 2;

drop materialized view mat1;


set hive.materializedview.rewriting.sql=true;

-- This MV cannot be used in any rewrite because of property rewriting.scope=''
create materialized view mat_none tblproperties ('rewriting.scope'='') as
select * from t1 where col0 = 2;

explain cbo
select * from t1 where col0 = 2;

drop materialized view mat_none;
