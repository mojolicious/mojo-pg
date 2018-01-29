use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use Mojo::IOLoop;
use Mojo::Pg;

# Isolate tests
my $pg = Mojo::Pg->new($ENV{TEST_ONLINE})->search_path(['mojo_crud_test']);
$pg->db->query('drop schema if exists mojo_crud_test cascade');
$pg->db->query('create schema mojo_crud_test');

my $db = $pg->db;
$db->query(
  'create table if not exists crud_test (
     id   serial primary key,
     name text
   )'
);

# Create
$db->insert('crud_test', {name => 'foo'});
is_deeply $db->select('crud_test')->hashes->to_array,
  [{id => 1, name => 'foo'}], 'right structure';
is $db->insert('crud_test', {name => 'bar'}, {returning => 'id'})->hash->{id},
  2, 'right value';
is_deeply $db->select('crud_test')->hashes->to_array,
  [{id => 1, name => 'foo'}, {id => 2, name => 'bar'}], 'right structure';
$db->insert('crud_test', {id => 1, name => 'foo'}, {on_conflict => undef});
$db->insert(
  'crud_test',
  {id => 2, name => 'bar'},
  {on_conflict => [id => {name => 'baz'}]}
);

# Read
is_deeply $db->select('crud_test')->hashes->to_array,
  [{id => 1, name => 'foo'}, {id => 2, name => 'baz'}], 'right structure';
is_deeply $db->select('crud_test', ['name'])->hashes->to_array,
  [{name => 'foo'}, {name => 'baz'}], 'right structure';
is_deeply $db->select('crud_test', ['name'], {name => 'foo'})->hashes->to_array,
  [{name => 'foo'}], 'right structure';
is_deeply $db->select('crud_test', ['name'], undef, {-desc => 'id'})
  ->hashes->to_array, [{name => 'baz'}, {name => 'foo'}], 'right structure';
is_deeply $db->select('crud_test', undef, undef, {offset => 1})
  ->hashes->to_array, [{id => 2, name => 'baz'}], 'right structure';
is_deeply $db->select('crud_test', undef, undef, {limit => 1})
  ->hashes->to_array, [{id => 1, name => 'foo'}], 'right structure';

# Non-blocking read
my $result;
my $delay = Mojo::IOLoop->delay(sub { $result = pop->hashes->to_array });
$db->select('crud_test', $delay->begin);
$delay->wait;
is_deeply $result, [{id => 1, name => 'foo'}, {id => 2, name => 'baz'}],
  'right structure';
$result = undef;
$delay = Mojo::IOLoop->delay(sub { $result = pop->hashes->to_array });
$db->select('crud_test', undef, undef, {-desc => 'id'}, $delay->begin);
$delay->wait;
is_deeply $result, [{id => 2, name => 'baz'}, {id => 1, name => 'foo'}],
  'right structure';

# Update
$db->update('crud_test', {name => 'yada'}, {name => 'foo'});
is_deeply $db->select('crud_test', undef, undef, {-asc => 'id'})
  ->hashes->to_array, [{id => 1, name => 'yada'}, {id => 2, name => 'baz'}],
  'right structure';

# Delete
$db->delete('crud_test', {name => 'yada'});
is_deeply $db->select('crud_test', undef, undef, {-asc => 'id'})
  ->hashes->to_array, [{id => 2, name => 'baz'}], 'right structure';
$db->delete('crud_test');
is_deeply $db->select('crud_test')->hashes->to_array, [], 'right structure';

# Quoting
$db->query(
  'create table if not exists crud_test2 (
     id   serial primary key,
     "t e s t" text
   )'
);
$db->insert('crud_test2',                {'t e s t' => 'foo'});
$db->insert('mojo_crud_test.crud_test2', {'t e s t' => 'bar'});
is_deeply $db->select('mojo_crud_test.crud_test2')->hashes->to_array,
  [{id => 1, 't e s t' => 'foo'}, {id => 2, 't e s t' => 'bar'}],
  'right structure';

# Arrays
$db->query(
  'create table if not exists crud_test3 (
     id   serial primary key,
     names text[]
   )'
);
$db->insert('crud_test3', {names => ['foo', 'bar']});
is_deeply $db->select('crud_test3')->hashes->to_array,
  [{id => 1, names => ['foo', 'bar']}], 'right structure';
$db->update('crud_test3', {names => ['foo', 'bar', 'baz', 'yada']}, {id => 1});
is_deeply $db->select('crud_test3')->hashes->to_array,
  [{id => 1, names => ['foo', 'bar', 'baz', 'yada']}], 'right structure';

# Promises
$result = undef;
$pg->db->insert_p('crud_test', {name => 'promise'}, {returning => '*'})
  ->then(sub { $result = shift->hash })->wait;
is $result->{name}, 'promise', 'right result';
$result = undef;
$db->select_p('crud_test', '*', {name => 'promise'})
  ->then(sub { $result = shift->hash })->wait;
is $result->{name}, 'promise', 'right result';
$result = undef;
my $first  = $pg->db->query_p("select * from crud_test where name = 'promise'");
my $second = $pg->db->query_p("select * from crud_test where name = 'promise'");
Mojo::Promise->all($first, $second)->then(
  sub {
    my ($first, $second) = @_;
    $result = [$first->[0]->hash, $second->[0]->hash];
  }
)->wait;
is $result->[0]{name}, 'promise', 'right result';
is $result->[1]{name}, 'promise', 'right result';
$result = undef;
$db->update_p(
  'crud_test',
  {name      => 'promise_two'},
  {name      => 'promise'},
  {returning => '*'}
)->then(sub { $result = shift->hash })->wait;
is $result->{name}, 'promise_two', 'right result';
$db->delete_p('crud_test', {name => 'promise_two'}, {returning => '*'})
  ->then(sub { $result = shift->hash })->wait;
is $result->{name}, 'promise_two', 'right result';

# Promises (rejected)
my $fail;
$db->dollar_only->query_p('does_not_exist')->catch(sub { $fail = shift })->wait;
like $fail, qr/does_not_exist/, 'right error';

# Join
$db->query(
  'create table if not exists crud_test4 (
     id    serial primary key,
     test1 text
   )'
);
$db->query(
  'create table if not exists crud_test5 (
     id    serial primary key,
     test2 text
   )'
);
$db->insert('crud_test4', {test1 => 'hello'});
$db->insert('crud_test5', {test2 => 'world'});
is_deeply $db->select(['crud_test4', ['crud_test5', id => 'id']],
  ['crud_test4.id', 'test1', 'test2', ['crud_test4.test1' => 'test3']])
  ->hashes->to_array,
  [{id => 1, test1 => 'hello', test2 => 'world', test3 => 'hello'}],
  'right structure';

# Clean up once we are done
$pg->db->query('drop schema mojo_crud_test cascade');

done_testing();
