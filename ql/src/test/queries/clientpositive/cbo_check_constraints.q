set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.cbo.enable=false;

--create table tcast(
--    url string NOT NULL ENABLE,
--    numClicks int,
--    price FLOAT CHECK (cast(numClicks as FLOAT)*price > 10.00));
create table tcast(
    url int,
    numClicks int,
    price int);

--EXPLAIN INSERT INTO tcast(url, price) values('www.yahoo.com', 0.5);
EXPLAIN INSERT INTO tcast(url, price) values(1, 5);
