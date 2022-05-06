set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table t1(a string, b int) clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(a, b) values
('in bucket 1', 1),
('move to bucket 1', 2),
('in bucket 1', 1),
('do not move to bucket 1', 2);

select t1.ROW__ID, t1.INPUT__FILE__NAME, * from t1;

update t1 set b = 1 where a = 'move to bucket 1';

select t1.ROW__ID, t1.INPUT__FILE__NAME, * from t1;
