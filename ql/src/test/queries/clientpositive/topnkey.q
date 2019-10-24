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

set hive.tez.dynamic.partition.pruning=true;
set hive.stats.fetch.column.stats=true;
set hive.cbo.enable=true;

EXPLAIN EXTENDED
SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;

SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;

EXPLAIN
SELECT key FROM src GROUP BY key ORDER BY key LIMIT 5;

SELECT key FROM src GROUP BY key ORDER BY key LIMIT 5;

