SET hive.vectorized.execution.enabled=false;

create table t1(a int, b int);

insert into t1(a, b) values
(1, 1), (2, 2), (3, 3), (null, null),
(1, 5), (2, 6), (3, 2),
(1, 5), (2, 6), (3, 2),
(1, NULL), (2, NULL), (3, NULL);


select a, last_value(b) ignore nulls over(partition by a order by b) from t1;