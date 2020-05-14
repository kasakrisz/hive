create table if not exists customer
(
    c_customer_sk             bigint,
    c_customer_id             int,
    c_first_name              string,
    c_last_name               string
);

create table store_sales
(
    ss_customer_sk            int,
    ss_customer_id            int,
    ss_quantity               int,
    ss_list_price             float
);

alter table customer add constraint pk_c primary key (c_customer_sk, c_customer_id) disable novalidate rely;


insert into customer(c_customer_sk, c_first_name, c_last_name)
values (1, 'Bud', 'Spencer');

insert into store_sales(ss_customer_sk, ss_quantity, ss_list_price)
values (1, 132, 10.5);


--explain cbo
--select ss.ss_quantity, c.c_first_name
--from store_sales ss
--join customer c on ss.ss_customer_sk = c.c_customer_sk
--;


explain cbo
select ss_customer_sk, ss_customer_id, c_last_name, ss_list_price, c_customer_sk, c_customer_id, c_first_name, ss_quantity
from store_sales ss
join customer c on ss.ss_customer_sk = c.c_customer_sk and ss_customer_id = c_customer_id
;

select ss_customer_sk, c_last_name, ss_list_price, c_customer_sk, c_first_name, ss_quantity
from store_sales ss
join customer c on ss.ss_customer_sk = c.c_customer_sk
;
