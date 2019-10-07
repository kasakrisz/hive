CREATE TABLE t_test3 (
  col1 int,
  col2 int
);
INSERT INTO t_test3 VALUES
--(NULL, NULL),
(3, 0),
(5, 1),
(5, 1),
(5, 2),
(5, 3),
(10, 20.0),
--(NULL, NULL),
--(NULL, NULL),
(11, 10.0),
(15, 7.0),
(15, 15.0),
(15, 16.0),
(8, 8.0),
(7, 7.0),
(8, 8.0);
--(NULL, NULL);


SELECT
--rank(5, 0) WITHIN GROUP (ORDER BY col1, col2),
--rank(5, 1) WITHIN GROUP (ORDER BY col1, col2),
--rank(5, 2) WITHIN GROUP (ORDER BY col1, col2),
rank(5, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(5, 3) WITHIN GROUP (ORDER BY col1, col2 DESC),
rank(5, 3) WITHIN GROUP (ORDER BY col1 DESC, col2),
rank(5, 3) WITHIN GROUP (ORDER BY col1 DESC, col2 DESC)
--rank(5, 4) WITHIN GROUP (ORDER BY col1, col2),
--rank(5, 5) WITHIN GROUP (ORDER BY col1, col2),
--rank(5, 6) WITHIN GROUP (ORDER BY col1, col2)
from t_test3;


--CREATE TABLE t_test (
--  int_col int,
--  double_col double
--);
--INSERT INTO t_test VALUES
--(NULL, NULL),
--(3, 3.0),
--(8, 8.0),
--(13, 13.0),
--(7, 7.0),
--(6, 6.0),
--(20, 20.0),
--(NULL, NULL),
--(NULL, NULL),
--(10, 10.0),
--(7, 7.0),
--(15, 15.0),
--(16, 16.0),
--(8, 8.0),
--(7, 7.0),
--(8, 8.0),
--(NULL, NULL);
--
----SELECT rank(6) WITHIN GROUP (ORDER BY CAST(int_col AS BIGINT))
----FROM t_test;
--
--SELECT
--rank(6) WITHIN GROUP (ORDER BY int_col),
--rank(6) WITHIN GROUP (ORDER BY int_col ASC),
--rank(6) WITHIN GROUP (ORDER BY int_col ASC NULLS FIRST),
--rank(6) WITHIN GROUP (ORDER BY int_col ASC NULLS LAST),
--rank(6) WITHIN GROUP (ORDER BY int_col DESC),
--rank(6) WITHIN GROUP (ORDER BY int_col DESC NULLS FIRST),
--rank(6) WITHIN GROUP (ORDER BY int_col DESC NULLS LAST)
--FROM t_test;
--
--SELECT
--rank(6.1) WITHIN GROUP (ORDER BY double_col),
--rank(6.1) WITHIN GROUP (ORDER BY double_col DESC)
--FROM t_test;
--
--SELECT
--rank(6.1) WITHIN GROUP (ORDER BY CAST(int_col AS double))
--FROM t_test;
--
--
--SELECT int_col, rank() OVER (ORDER BY int_col)
--FROM t_test;
--
--SELECT int_col, rank() OVER (ORDER BY int_col DESC)
--FROM t_test;
--
----set hive.map.aggr = false;
----set hive.groupby.skewindata = false;
----
------ SORT_QUERY_RESULTS
----
----select
----rankwg(1) within group (order by value),
----rankwg(2) within group (order by value),
----rankwg(3) within group (order by value),
----rankwg(4) within group (order by value),
----rankwg(5) within group (order by value),
----rankwg(6) within group (order by value),
----rankwg(7) within group (order by value),
----rankwg(8) within group (order by value),
----rankwg(9) within group (order by value),
----rankwg(10) within group (order by value),
----rankwg(11) within group (order by value),
----rankwg(12) within group (order by value),
----rankwg(13) within group (order by value),
----rankwg(14) within group (order by value),
----rankwg(15) within group (order by value),
----rankwg(16) within group (order by value),
----rankwg(30) within group (order by value)
----from t_test;
----
----
----set hive.map.aggr = true;
----set hive.groupby.skewindata = false;
----
----select
----rankwg(1) within group (order by value),
----rankwg(2) within group (order by value),
----rankwg(3) within group (order by value),
----rankwg(4) within group (order by value),
----rankwg(5) within group (order by value),
----rankwg(6) within group (order by value),
----rankwg(7) within group (order by value),
----rankwg(8) within group (order by value),
----rankwg(9) within group (order by value),
----rankwg(10) within group (order by value),
----rankwg(11) within group (order by value),
----rankwg(12) within group (order by value),
----rankwg(13) within group (order by value),
----rankwg(14) within group (order by value),
----rankwg(15) within group (order by value),
----rankwg(16) within group (order by value),
----rankwg(30) within group (order by value)
----from t_test;
----
----
----
----set hive.map.aggr = false;
----set hive.groupby.skewindata = true;
----
----select
----rankwg(1) within group (order by value),
----rankwg(2) within group (order by value),
----rankwg(3) within group (order by value),
----rankwg(4) within group (order by value),
----rankwg(5) within group (order by value),
----rankwg(6) within group (order by value),
----rankwg(7) within group (order by value),
----rankwg(8) within group (order by value),
----rankwg(9) within group (order by value),
----rankwg(10) within group (order by value),
----rankwg(11) within group (order by value),
----rankwg(12) within group (order by value),
----rankwg(13) within group (order by value),
----rankwg(14) within group (order by value),
----rankwg(15) within group (order by value),
----rankwg(16) within group (order by value),
----rankwg(30) within group (order by value)
----from t_test;
----
----
----set hive.map.aggr = true;
----set hive.groupby.skewindata = true;
----
----select
----rankwg(1) within group (order by value),
----rankwg(2) within group (order by value),
----rankwg(3) within group (order by value),
----rankwg(4) within group (order by value),
----rankwg(5) within group (order by value),
----rankwg(6) within group (order by value),
----rankwg(7) within group (order by value),
----rankwg(8) within group (order by value),
----rankwg(9) within group (order by value),
----rankwg(10) within group (order by value),
----rankwg(11) within group (order by value),
----rankwg(12) within group (order by value),
----rankwg(13) within group (order by value),
----rankwg(14) within group (order by value),
----rankwg(15) within group (order by value),
----rankwg(16) within group (order by value),
----rankwg(30) within group (order by value)
----from t_test;
----
----
----set hive.map.aggr = true;
----set hive.groupby.skewindata = false;
----
----
----DROP TABLE t_test;
