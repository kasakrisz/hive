create table t1(a int, b int);

insert into t1(a, b) values
(1000, 1000),
(2000, 2000),
(3000, NULL),
(4000, 4000),
(5000, NULL),
(6000, NULL),
(7000, 7000),
(8000, 8000),
(9000, 9000)
;

select
    b,
--    lead(b, 2) over (order by a desc),
    lead(b, 2) ignore nulls over (order by a desc)
--    lead(b, 3) ignore nulls over (order by a desc)
from t1;
