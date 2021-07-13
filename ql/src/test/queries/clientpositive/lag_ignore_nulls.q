create table t1(a int, b int);

insert into t1(a, b) values
(1000, 1000),
(2000, 2000),
(3000, NULL),
(4000, NULL),
(5000, 5000),
(6000, 6000),
(7000, NULL),
(8000, 8000)
;

select
    b,
    lag(b, 2) over (order by a desc),
    lag(b, 2) ignore nulls over (order by a desc),
    lag(b, 3) ignore nulls over (order by a desc)
from t1;
