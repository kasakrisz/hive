create table if not exists customer
(
    c_customer_sk             bigint   primary key disable,
    c_first_name              string,
    c_last_name               string
);

insert into customer(c_customer_sk, c_first_name, c_last_name)
values (1, 'Bud', 'Spencer');

create table store_sales
(
    ss_customer_sk            int      primary key disable,
    ss_quantity               int,
    ss_list_price             float
);

insert into store_sales(ss_customer_sk, ss_quantity, ss_list_price)
values (1, 132, 10.5);


explain cbo
select ss.ss_quantity, c.c_first_name
from store_sales ss
join customer c on ss.ss_customer_sk = c.c_customer_sk
;


explain cbo
select ss_customer_sk, c_last_name, ss_list_price, c_customer_sk, c_first_name, ss_quantity
from store_sales ss
join customer c on ss.ss_customer_sk = c.c_customer_sk
;

select ss_customer_sk, c_last_name, ss_list_price, c_customer_sk, c_first_name, ss_quantity
from store_sales ss
join customer c on ss.ss_customer_sk = c.c_customer_sk
;
