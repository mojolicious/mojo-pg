use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use File::Spec::Functions 'catfile';
use FindBin;
use Mojo::Pg;

# Isolate tests
my $pg
  = Mojo::Pg->new($ENV{TEST_ONLINE})->search_path(['mojo_migrations_test']);
$pg->db->query('drop schema if exists mojo_migrations_test cascade');
$pg->db->query('create schema mojo_migrations_test');

# Defaults
is $pg->migrations->name,   'migrations', 'right name';
is $pg->migrations->latest, 0,            'latest version is 0';
is $pg->migrations->active, 0,            'active version is 0';

# Create migrations table
ok !(grep {/^mojo_migrations_test\.mojo_migrations$/} @{$pg->db->tables}),
  'migrations table does not exist';
is $pg->migrations->migrate->active, 0, 'active version is 0';
ok !!(grep {/^mojo_migrations_test\.mojo_migrations$/} @{$pg->db->tables}),
  'migrations table exists';

# Migrations from DATA section
is $pg->migrations->from_data->latest, 0, 'latest version is 0';
is $pg->migrations->from_data(__PACKAGE__)->latest, 0, 'latest version is 0';
is $pg->migrations->name('test1')->from_data->latest, 10,
  'latest version is 10';
is $pg->migrations->name('test2')->from_data->latest, 2, 'latest version is 2';
is $pg->migrations->name('migrations')->from_data(__PACKAGE__, 'test1')->latest,
  10, 'latest version is 10';
is $pg->migrations->name('test2')->from_data(__PACKAGE__)->latest, 2,
  'latest version is 2';

# Different syntax variations
$pg->migrations->name('migrations_test')->from_string(<<EOF);
-- 1 up
create table if not exists migration_test_one (foo varchar(255));

-- 1down

  drop table if exists migration_test_one;

  -- 2 up

insert into migration_test_one values ('works ♥');
-- 2 down
delete from migration_test_one where foo = 'works ♥';
--
--  3 Up, create
--        another
--        table?
create table if not exists migration_test_two (bar varchar(255));
--3  DOWN
drop table if exists migration_test_two;

-- 10 up (not down)
insert into migration_test_two values ('works too');
-- 10 down (not up)
delete from migration_test_two where bar = 'works too';
EOF
is $pg->migrations->latest, 10, 'latest version is 10';
is $pg->migrations->active, 0,  'active version is 0';
is $pg->migrations->migrate->active, 10, 'active version is 10';
ok !!(grep {/^mojo_migrations_test\.migration_test_one$/} @{$pg->db->tables}),
  'first table exists';
ok !!(grep {/^mojo_migrations_test\.migration_test_two$/} @{$pg->db->tables}),
  'second table exists';
is_deeply $pg->db->query('select * from migration_test_one')->hash,
  {foo => 'works ♥'}, 'right structure';
is $pg->migrations->migrate->active, 10, 'active version is 10';
is $pg->migrations->migrate(1)->active, 1, 'active version is 1';
is $pg->db->query('select * from migration_test_one')->hash, undef, 'no result';
is $pg->migrations->migrate(3)->active, 3, 'active version is 3';
is $pg->db->query('select * from migration_test_two')->hash, undef, 'no result';
is $pg->migrations->migrate->active, 10, 'active version is 10';
is_deeply $pg->db->query('select * from migration_test_two')->hash,
  {bar => 'works too'}, 'right structure';
is $pg->migrations->migrate(0)->active, 0, 'active version is 0';

# Bad and concurrent migrations
my $pg2
  = Mojo::Pg->new($ENV{TEST_ONLINE})->search_path(['mojo_migrations_test']);
$pg2->migrations->name('migrations_test2')
  ->from_file(catfile($FindBin::Bin, 'migrations', 'test.sql'));
is $pg2->migrations->latest, 4, 'latest version is 4';
is $pg2->migrations->active, 0, 'active version is 0';
eval { $pg2->migrations->migrate };
like $@, qr/does_not_exist/, 'right error';
is $pg2->migrations->migrate(3)->active, 3, 'active version is 3';
is $pg2->migrations->migrate(2)->active, 2, 'active version is 2';
is $pg->migrations->active, 0, 'active version is still 0';
is $pg->migrations->migrate->active, 10, 'active version is 10';
is_deeply $pg2->db->query('select * from migration_test_three')
  ->hashes->to_array, [{baz => 'just'}, {baz => 'works ♥'}],
  'right structure';
is $pg->migrations->migrate(0)->active,  0, 'active version is 0';
is $pg2->migrations->migrate(0)->active, 0, 'active version is 0';

# Migrate automatically
my $pg3
  = Mojo::Pg->new($ENV{TEST_ONLINE})->search_path(['mojo_migrations_test']);
$pg3->migrations->name('migrations_test')->from_string(<<EOF);
-- 5 up
create table if not exists migration_test_six (foo varchar(255));
-- 6 up
insert into migration_test_six values ('works!');
-- 5 down
drop table if exists migration_test_six;
-- 6 down
delete from migration_test_six;
EOF
$pg3->auto_migrate(1)->db;
is $pg3->migrations->active, 6, 'active version is 6';
is_deeply $pg3->db->query('select * from migration_test_six')->hashes,
  [{foo => 'works!'}], 'right structure';
is $pg3->migrations->migrate(5)->active, 5, 'active version is 5';
is_deeply $pg3->db->query('select * from migration_test_six')->hashes, [],
  'right structure';
is $pg3->migrations->migrate(0)->active, 0, 'active version is 0';
is $pg3->migrations->sql_for(0, 5), <<EOF, 'right SQL';
-- 5 up
create table if not exists migration_test_six (foo varchar(255));
EOF
is $pg3->migrations->sql_for(6, 0), <<EOF, 'right SQL';
-- 6 down
delete from migration_test_six;
-- 5 down
drop table if exists migration_test_six;
EOF
is $pg3->migrations->sql_for(6, 5), <<EOF, 'right SQL';
-- 6 down
delete from migration_test_six;
EOF
is $pg3->migrations->sql_for(6, 6), '', 'right SQL';
is $pg3->migrations->sql_for(2, 3), '', 'right SQL';

# Migrate automatically with shared connection cache
my $pg4
  = Mojo::Pg->new($ENV{TEST_ONLINE})->search_path(['mojo_migrations_test']);
my $pg5 = Mojo::Pg->new($pg4);
$pg4->auto_migrate(1)->migrations->name('test1')->from_data;
$pg5->auto_migrate(1)->migrations->name('test3')->from_data;
is_deeply $pg5->db->query('select * from migration_test_four')
  ->hashes->to_array, [{test => 10}], 'right structure';
is_deeply $pg5->db->query('select * from migration_test_six')->hashes->to_array,
  [], 'right structure';

# Unknown version
eval { $pg->migrations->migrate(23) };
like $@, qr/Version 23 has no migration/, 'right error';

# Version mismatch
my $newer = <<EOF;
-- 2 up
create table migration_test_five (test int);
-- 2 down
drop table migration_test_five;
EOF
$pg->migrations->name('migrations_test3')->from_string($newer);
is $pg->migrations->migrate->active, 2, 'active version is 2';
$pg->migrations->from_string(<<EOF);
-- 1 up
create table migration_test_five (test int);
EOF
eval { $pg->migrations->migrate };
like $@, qr/Active version 2 is greater than the latest version 1/,
  'right error';
eval { $pg->migrations->migrate(0) };
like $@, qr/Active version 2 is greater than the latest version 1/,
  'right error';
is $pg->migrations->from_string($newer)->migrate(0)->active, 0,
  'active version is 0';

# Clean up once we are done
$pg->db->query('drop schema mojo_migrations_test cascade');

done_testing();

__DATA__
@@ test1
-- 7 up
create table migration_test_four (test int);

-- 10 up
insert into migration_test_four values (10);

@@ test2
-- 2 up
create table migration_test_five (test int);

@@ test3
-- 2 up
create table migration_test_six (test int);
