-- SORT_QUERY_RESULTS

create external table target_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='2');
create table source(a int, b string, c int);

insert into target_ice values (1, 'one', 50), (2, 'two', 51), (111, 'one', 55), (333, 'two', 56);
insert into source values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55);

select * from target_ice;

-- merge
explain
merge into target_ice as t using source src ON t.a = src.a
when matched and t.a > 100 THEN DELETE
when matched then update set b = 'Merged', c = t.c + 10
when not matched then insert values (src.a, src.b, src.c);

merge into target_ice as t using source src ON t.a = src.a
when matched and t.a > 100 THEN DELETE
when matched then update set b = 'Merged', c = t.c + 10
when not matched then insert values (src.a, src.b, src.c);

--merge into target_ice as t using source src ON t.a = src.a
--when matched then update set b = 'Merged', c = t.c + 10;

select * from target_ice;

---- target not aliased
--merge into target_ice using source src ON target_ice.a = src.a
--when matched and src.a > 100 THEN DELETE
--when matched then update set b = 'Merged', c = target_ice.c + 10
--when not matched then insert values (src.a, src.b, src.c);
--
--select * from target_ice;
