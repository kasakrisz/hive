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

set hive.stats.fetch.column.stats=true;
set hive.cbo.enable=true;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;
SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;

set hive.optimize.topnkey=false;
SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;

CREATE TABLE t_test(
  a int,
  b int,
  c int
);

INSERT INTO t_test VALUES
(5, 2, 3),
(6, 2, 1),
(7, 8, 4), (7, 8, 4), (7, 8, 4),
(5, 1, 2), (5, 1, 2), (5, 1, 2);

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test ORDER BY a, b LIMIT 3;
SELECT a, b FROM t_test ORDER BY a, b LIMIT 3;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test ORDER BY a, b LIMIT 3;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test GROUP BY a, b ORDER BY a, b LIMIT 3;
SELECT a, b FROM t_test GROUP BY a, b ORDER BY a, b LIMIT 3;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test GROUP BY a, b ORDER BY a, b LIMIT 3;

DROP TABLE t_test;
