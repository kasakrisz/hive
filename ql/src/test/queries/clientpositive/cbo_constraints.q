set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
--set hive.cbo.enable=false;

create table acid_uami(i int,
                 de decimal(5,2) constraint nn1 not null enforced,
                 vc varchar(128) constraint nn2 not null enforced) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

--explain
--insert into acid_uami values(1, NULL, 'first');
explain
insert into acid_uami(i, de) values(1, 1.4);

explain cbo
insert into acid_uami values(1, 1.4, 'first');
explain
insert into acid_uami values(1, 1.4, 'first');
insert into acid_uami values(1, 1.4, 'first');

--explain
--update acid_uami set de=null where i=1;
--explain
--UPDATE acid_uami set de=de + 20 where i=1;
