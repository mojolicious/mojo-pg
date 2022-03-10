use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use DBD::Pg qw(:pg_types);
use Mojo::Pg;
use Mojo::Promise;
use Mojo::Util qw(encode);

package MojoPgTest::Database;
use Mojo::Base 'Mojo::Pg::Database';

sub results_class {'MojoPgTest::Results'}

package MojoPgTest::Results;
use Mojo::Base 'Mojo::Pg::Results';

sub array_test { shift->array }

package main;

# Isolate tests
my $pg = Mojo::Pg->new($ENV{TEST_ONLINE})->search_path(['mojo_results_test']);
$pg->db->query('DROP SCHEMA IF EXISTS mojo_results_test CASCADE');
$pg->db->query('CREATE SCHEMA mojo_results_test');

my $db = $pg->db;
is_deeply $pg->search_path, ['mojo_results_test'], 'right search path';
$db->query(
  'CREATE TABLE IF NOT EXISTS results_test (
     id   SERIAL PRIMARY KEY,
     name TEXT
   )'
);
$db->query('INSERT INTO results_test (name) VALUES (?)', $_) for qw(foo bar);

subtest 'Tables' => sub {
  ok !!(grep {/^mojo_results_test\.results.test$/} @{$db->tables}), 'results table exists';
  ok !(grep {/^information_schema\.tables$/} @{$db->tables}),       'internal tables are hidden';
  ok !(grep {/^pg_catalog\.pg_tables$/} @{$db->tables}),            'internal tables are hidden';
};

subtest 'Result methods' => sub {
  is_deeply $db->query('SELECT * FROM results_test')->rows,             2, 'two rows';
  is_deeply $db->query('SELECT * FROM results_test')->columns,          ['id', 'name'], 'right structure';
  is_deeply $db->query('SELECT * FROM results_test')->array,            [1,    'foo'],  'right structure';
  is_deeply $db->query('SELECT * FROM results_test')->arrays->to_array, [[1, 'foo'], [2, 'bar']], 'right structure';
  is_deeply $db->query('SELECT * FROM results_test')->hash, {id => 1, name => 'foo'}, 'right structure';
  is_deeply $db->query('SELECT * FROM results_test')->hashes->to_array,
    [{id => 1, name => 'foo'}, {id => 2, name => 'bar'}], 'right structure';
  is $pg->db->query('SELECT * FROM results_test')->text, "1  foo\n2  bar\n", 'right text';
};

subtest 'Custom database and results classes' => sub {
  is ref $db, 'Mojo::Pg::Database', 'right class';
  $pg->database_class('MojoPgTest::Database');
  $db = $pg->db;
  is ref $db,                    'MojoPgTest::Database', 'right class';
  is ref $db->query('SELECT 1'), 'MojoPgTest::Results',  'right class';
  is_deeply $db->query('SELECT * from results_test')->array_test, [1, 'foo'], 'right structure';
};

subtest 'JSON' => sub {
  is_deeply $db->query('SELECT ?::JSON AS foo', {json => {bar => 'baz'}})->expand->hash, {foo => {bar => 'baz'}},
    'right structure';
  is_deeply $db->query('SELECT ?::JSON AS foo', {-json => {bar => 'baz'}})->expand->hash, {foo => {bar => 'baz'}},
    'right structure';
  is_deeply $db->query('SELECT ?::JSON AS foo', {json => {bar => 'baz'}})->expand->array, [{bar => 'baz'}],
    'right structure';
  my $hashes = [{foo => {one => 1}, bar => 'a'}, {foo => {two => 2}, bar => 'b'}];
  is_deeply $db->query(
    "SELECT 'a' AS bar, ?::JSON AS foo
     UNION ALL
     SELECT 'b' AS bar, ?::JSON AS foo", {json => {one => 1}}, {json => {two => 2}}
  )->expand->hashes->to_array, $hashes, 'right structure';
  my $arrays = [['a', {one => 1}], ['b', {two => 2}]];
  is_deeply $db->query(
    "SELECT 'a' AS bar, ?::JSON AS foo
     UNION ALL
     SELECT 'b' AS bar, ?::JSON AS foo", {json => {one => 1}}, {json => {two => 2}}
  )->expand->arrays->to_array, $arrays, 'right structure';
};

subtest 'Iterate' => sub {
  my $results = $db->query('SELECT * FROM results_test');
  is_deeply $results->array, [1, 'foo'], 'right structure';
  is_deeply $results->array, [2, 'bar'], 'right structure';
  is $results->array, undef, 'no more results';
};

subtest 'Non-blocking query where not all results have been fetched' => sub {
  my ($fail, $result);
  $db->query_p('SELECT name FROM results_test')->then(sub {
    my $results = shift;
    push @$result, $results->array;
    $results->finish;
    return $db->query_p('SELECT name FROM results_test');
  })->then(sub {
    my $results = shift;
    push @$result, $results->array_test;
    $results->finish;
    return $db->query_p('SELECT name FROM results_test');
  })->then(sub {
    my $results = shift;
    push @$result, $results->array;
  })->catch(sub { $fail = shift })->wait;
  ok !$fail, 'no error';
  is_deeply $result, [['foo'], ['foo'], ['foo']], 'right structure';
};

subtest 'Transactions' => sub {
  {
    my $tx = $db->begin;
    $db->query("INSERT INTO results_test (name) VALUES ('tx1')");
    $db->query("INSERT INTO results_test (name) VALUES ('tx1')");
    $tx->commit;
  };
  is_deeply $db->query('SELECT * FROM results_test WHERE name = ?', 'tx1')->hashes->to_array,
    [{id => 3, name => 'tx1'}, {id => 4, name => 'tx1'}], 'right structure';
  {
    my $tx = $db->begin;
    $db->query("INSERT INTO results_test (name) VALUES ('tx2')");
    $db->query("INSERT INTO results_test (name) VALUES ('tx2')");
  };
  is_deeply $db->query('SELECT * FROM results_test WHERE name = ?', 'tx2')->hashes->to_array, [], 'no results';
  eval {
    my $tx = $db->begin;
    $db->query("INSERT INTO results_test (name) VALUES ('tx3')");
    $db->query("INSERT INTO results_test (name) VALUES ('tx3')");
    $db->query('does_not_exist');
    $tx->commit;
  };
  like $@, qr/does_not_exist/, 'right error';
  is_deeply $db->query('SELECT * FROM results_test WHERE name = ?', 'tx3')->hashes->to_array, [], 'no results';
};

subtest 'Long-lived results' => sub {
  my $results1 = $db->query('SELECT 1 AS one');
  is_deeply $results1->hashes, [{one => 1}], 'right structure';
  my $results2 = $db->query('SELECT 1 AS one');
  undef $results1;
  is_deeply $results2->hashes, [{one => 1}], 'right structure';
};

subtest 'Custom data types' => sub {
  $db->query('CREATE TABLE IF NOT EXISTS results_test2 (stuff BYTEA)');
  my $snowman = encode 'UTF-8', '☃';
  $db->query('INSERT INTO results_test2 (stuff) VALUES (?)', {value => $snowman, type => PG_BYTEA});
  is_deeply $db->query('SELECT * FROM results_test2')->hash, {stuff => $snowman}, 'right structure';
};

# Clean up once we are done
$pg->db->query('DROP SCHEMA mojo_results_test CASCADE');

done_testing();
