set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE t1(a int, b int, c int, d string, e float) STORED AS ORC TBLPROPERTIES ('transactional' = 'true');

INSERT INTO t1(a, b, c, d, e) VALUES
(1, 1, 1, 'one', 1.1),
(1, 4, 1, 'one', 4.2),
(2, 2, 2, 'two', 2.2),
(1, 10, 1, 'one', 10.1),
(2, 2, 2, 'two', 2.2),
(1, 3, 1, 'one', 3.1),
(null, 4, null, 'unknown', 4.6),
(null, 4, 2, 'unknown', 4.7),
(2, 1, 2, 'two', 5.6),
(2, 1, 2, 'two', 7.14),
(2, 1, 2, 'two', 17.4),
(2, 1, 2, 'two', 7.4),
(2, 4, 2, 'two', 7.4),
(2, 4, 2, 'two', 8.22),
(2, 4, 2, 'two', 7.4);

CREATE MATERIALIZED VIEW mat1 PARTITIONED ON (a, c, d) STORED AS ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') AS
SELECT a, sum(b) sumb, c, d, sum(e) sume FROM t1 GROUP BY a, c, d;

INSERT INTO t1(a, b, c, d, e) VALUES
(1, 3, 1, 'one', 3.3),
(1, 110, 1, 'one', 110.11),
(null, 20, null, 'unknown', 20.22);

EXPLAIN CBO
ALTER MATERIALIZED VIEW mat1 REBUILD;
EXPLAIN
ALTER MATERIALIZED VIEW mat1 REBUILD;
ALTER MATERIALIZED VIEW mat1 REBUILD;

EXPLAIN CBO
SELECT a, sum(b), c, d, sum(e) FROM t1 GROUP BY a, c, d
ORDER BY a, c, d;

SELECT a, sum(b), c, d, sum(e) FROM t1 GROUP BY a, c, d
ORDER BY a, c, d;

DROP MATERIALIZED VIEW mat1;

-- Uncomment this to compare results when view is used/not used
--SELECT a, sum(b), c, d, sum(e) FROM t1 GROUP BY a, c, d
--ORDER BY a, c, d;
