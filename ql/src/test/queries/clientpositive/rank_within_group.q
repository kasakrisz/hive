CREATE TABLE t_test (
  int_col int,
  double_col double
);
INSERT INTO t_test VALUES
(NULL, NULL),
(3, 3.0),
(8, 8.0),
(13, 13.0),
(7, 7.0),
(6, 6.0),
(20, 20.0),
(NULL, NULL),
(NULL, NULL),
(10, 10.0),
(7, 7.0),
(15, 15.0),
(16, 16.0),
(8, 8.0),
(7, 7.0),
(8, 8.0),
(NULL, NULL);

--SELECT rank(6) WITHIN GROUP (ORDER BY CAST(int_col AS BIGINT))
--FROM t_test;

SELECT rank(6.1) WITHIN GROUP (ORDER BY double_col)
FROM t_test;

SELECT rank(6.1) WITHIN GROUP (ORDER BY CAST(int_col AS double))
FROM t_test;


--SELECT value, rank() OVER (order by CAST(value AS DOUBLE))
--FROM t_test;

--set hive.map.aggr = false;
--set hive.groupby.skewindata = false;
--
---- SORT_QUERY_RESULTS
--
--select
--rankwg(1) within group (order by value),
--rankwg(2) within group (order by value),
--rankwg(3) within group (order by value),
--rankwg(4) within group (order by value),
--rankwg(5) within group (order by value),
--rankwg(6) within group (order by value),
--rankwg(7) within group (order by value),
--rankwg(8) within group (order by value),
--rankwg(9) within group (order by value),
--rankwg(10) within group (order by value),
--rankwg(11) within group (order by value),
--rankwg(12) within group (order by value),
--rankwg(13) within group (order by value),
--rankwg(14) within group (order by value),
--rankwg(15) within group (order by value),
--rankwg(16) within group (order by value),
--rankwg(30) within group (order by value)
--from t_test;
--
--
--set hive.map.aggr = true;
--set hive.groupby.skewindata = false;
--
--select
--rankwg(1) within group (order by value),
--rankwg(2) within group (order by value),
--rankwg(3) within group (order by value),
--rankwg(4) within group (order by value),
--rankwg(5) within group (order by value),
--rankwg(6) within group (order by value),
--rankwg(7) within group (order by value),
--rankwg(8) within group (order by value),
--rankwg(9) within group (order by value),
--rankwg(10) within group (order by value),
--rankwg(11) within group (order by value),
--rankwg(12) within group (order by value),
--rankwg(13) within group (order by value),
--rankwg(14) within group (order by value),
--rankwg(15) within group (order by value),
--rankwg(16) within group (order by value),
--rankwg(30) within group (order by value)
--from t_test;
--
--
--
--set hive.map.aggr = false;
--set hive.groupby.skewindata = true;
--
--select
--rankwg(1) within group (order by value),
--rankwg(2) within group (order by value),
--rankwg(3) within group (order by value),
--rankwg(4) within group (order by value),
--rankwg(5) within group (order by value),
--rankwg(6) within group (order by value),
--rankwg(7) within group (order by value),
--rankwg(8) within group (order by value),
--rankwg(9) within group (order by value),
--rankwg(10) within group (order by value),
--rankwg(11) within group (order by value),
--rankwg(12) within group (order by value),
--rankwg(13) within group (order by value),
--rankwg(14) within group (order by value),
--rankwg(15) within group (order by value),
--rankwg(16) within group (order by value),
--rankwg(30) within group (order by value)
--from t_test;
--
--
--set hive.map.aggr = true;
--set hive.groupby.skewindata = true;
--
--select
--rankwg(1) within group (order by value),
--rankwg(2) within group (order by value),
--rankwg(3) within group (order by value),
--rankwg(4) within group (order by value),
--rankwg(5) within group (order by value),
--rankwg(6) within group (order by value),
--rankwg(7) within group (order by value),
--rankwg(8) within group (order by value),
--rankwg(9) within group (order by value),
--rankwg(10) within group (order by value),
--rankwg(11) within group (order by value),
--rankwg(12) within group (order by value),
--rankwg(13) within group (order by value),
--rankwg(14) within group (order by value),
--rankwg(15) within group (order by value),
--rankwg(16) within group (order by value),
--rankwg(30) within group (order by value)
--from t_test;
--
--
--set hive.map.aggr = true;
--set hive.groupby.skewindata = false;
--
--
--DROP TABLE t_test;
