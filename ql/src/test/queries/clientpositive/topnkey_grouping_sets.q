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

CREATE TABLE t_test(
  a int,
  b int,
  c int
);

INSERT INTO t_test VALUES
--(NULL, NULL, NULL),
(5, 2, 3),
--(NULL, NULL, NULL),
--(NULL, NULL, NULL),
(6, 2, 1),
(7, 8, 4), (7, 8, 4), (7, 8, 4),
(5, 1, 2), (5, 1, 2), (5, 1, 2);
--(NULL, NULL, NULL);

explain
SELECT a, b, grouping(a), grouping(b), grouping(a, b) FROM t_test GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY a LIMIT 3;
SELECT a, b, grouping(a), grouping(b), grouping(a, b) FROM t_test GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY a LIMIT 3;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY a LIMIT 3;

set hive.optimize.topnkey=true;
SELECT a, b FROM t_test GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY a LIMIT 10;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY a LIMIT 10;

set hive.optimize.topnkey=true;
SELECT a, b FROM t_test GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY b LIMIT 3;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY b LIMIT 3;


DROP TABLE IF EXISTS tstore;
