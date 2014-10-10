-- 1 up
create table if not exists migration_test_three (baz varchar(255));
-- 1 down
drop table if exists migration_test_three;
-- 2 up
insert into migration_test_three values ('just');
insert into migration_test_three values ('works');
-- 3 up
-- 4 up
does_not_exist;
