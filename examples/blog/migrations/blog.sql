-- 1 up
create table posts (id serial primary key, title text, body text);

-- 1 down
drop table posts;
