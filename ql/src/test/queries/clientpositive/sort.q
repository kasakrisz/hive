--! qt:dataset:src
-- SORT_QUERY_RESULTS

SELECT x.key, x.value FROM SRC x ORDER BY 1;

SELECT x.key, x.value FROM SRC x SORT BY 1;

--EXPLAIN
--SELECT x.* FROM SRC x SORT BY key;

SELECT x.* FROM SRC x SORT BY key;
