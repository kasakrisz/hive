-- Test partition based MV rebuild when source table is insert only

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE t1(a int, b int,c int) STORED AS ORC TBLPROPERTIES ('transactional' = 'true', 'transactional_properties'='insert_only');

INSERT INTO t1(a, b, c) VALUES
(1, 1, 1),
(1, 1, 4),
(2, 1, 2),
(2, 1, 2),
(2, 1, 2),
(2, 1, 2),
(2, 1, 2),
(2, 4, 12),
(2, 4, 23),
(2, 4, 2),
(1, 2, 10),
(2, 2, 11),
(1, 3, 100),
(null, 4, 200);

CREATE MATERIALIZED VIEW mat1 PARTITIONED ON (a) STORED AS ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') AS
SELECT a, b, sum(c) sumc FROM t1 GROUP BY b, a;

INSERT INTO t1(a, b, c) VALUES
(1, 1, 3),
(1, 3, 110),
(null, 4, 20);

EXPLAIN CBO
ALTER MATERIALIZED VIEW mat1 REBUILD;
EXPLAIN
ALTER MATERIALIZED VIEW mat1 REBUILD;
ALTER MATERIALIZED VIEW mat1 REBUILD;

EXPLAIN CBO
SELECT b, sum(c), a sumc FROM t1 GROUP BY b, a
ORDER BY a, b;

SELECT b, sum(c), a sumc FROM t1 GROUP BY b, a
ORDER BY a, b;

DROP MATERIALIZED VIEW mat1;

-- Uncomment this to compare results when view is used/not used
--SELECT b, sum(c), a sumc FROM t1 GROUP BY b, a
--ORDER BY a, b;
