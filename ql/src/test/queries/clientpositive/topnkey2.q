--! qt:dataset:src
--set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=true;


set hive.optimize.topnkey=false;

--EXPLAIN SELECT key FROM src ORDER BY key LIMIT 5;
--SELECT key FROM src ORDER BY key LIMIT 5;

EXPLAIN SELECT key FROM src GROUP BY key ORDER BY key LIMIT 5;
SELECT key FROM src GROUP BY key ORDER BY key LIMIT 5;

set hive.optimize.topnkey=true;

--EXPLAIN SELECT key FROM src ORDER BY key LIMIT 5;
--SELECT key FROM src ORDER BY key LIMIT 5;

EXPLAIN SELECT key FROM src GROUP BY key ORDER BY key LIMIT 5;
SELECT key FROM src GROUP BY key ORDER BY key LIMIT 5;
