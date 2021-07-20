create table t1(a int, b int);

insert into t1(a, b) values
(1000, 1000),
(2000, 2000),
(2000, 2000),
(3000, NULL),
(4000, 4000),
(5000, NULL),
(6000, NULL),
(7000, NULL),
(8000, 8000),
(8000, 8000)
;

explain cbo
select
    b,
    lead(b, 4, a / 10) ignore nulls over (order by a desc)
from t1;

select
    b,
    lead(b, 4, a / 10) ignore nulls over (order by a desc)
from t1;


select
    b,
    lead(b, 2) ignore nulls over (order by a desc),
    lead(b, 2, 222) ignore nulls over (order by a desc),
    lead(b, 4) ignore nulls over (order by a desc),
    lead(b, 4, 333) ignore nulls over (order by a desc)
from t1;
