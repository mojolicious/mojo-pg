use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use Mojo::Pg;

# Isolate tests
my $pg = Mojo::Pg->new($ENV{TEST_ONLINE});
$pg->db->query('drop schema if exists mojo_results_test cascade');
$pg->db->query('create schema mojo_results_test');
$pg = Mojo::Pg->new($ENV{TEST_ONLINE})->search_path(['mojo_results_test']);

my $db = $pg->db;
$db->query(
  'create table if not exists results_test (
     id   serial primary key,
     name text
   )'
);
$db->query('insert into results_test (name) values (?)', $_) for qw(foo bar);

# Tables
ok !!(grep {/^mojo_results_test\.results.test$/} @{$db->tables}),
  'results table exists';
ok !(grep {/^information_schema\.tables$/} @{$db->tables}),
  'internal tables are hidden';
ok !(grep {/^pg_catalog\.pg_tables$/} @{$db->tables}),
  'internal tables are hidden';

# Result methods
is_deeply $db->query('select * from results_test')->rows, 2, 'two rows';
is_deeply $db->query('select * from results_test')->columns, ['id', 'name'],
  'right structure';
is_deeply $db->query('select * from results_test')->array, [1, 'foo'],
  'right structure';
is_deeply $db->query('select * from results_test')->arrays->to_array,
  [[1, 'foo'], [2, 'bar']], 'right structure';
is_deeply $db->query('select * from results_test')->hash,
  {id => 1, name => 'foo'}, 'right structure';
is_deeply $db->query('select * from results_test')->hashes->to_array,
  [{id => 1, name => 'foo'}, {id => 2, name => 'bar'}], 'right structure';
is $pg->db->query('select * from results_test')->text, "1  foo\n2  bar\n",
  'right text';

# Iterate
my $results = $db->query('select * from results_test');
is_deeply $results->array, [1, 'foo'], 'right structure';
is_deeply $results->array, [2, 'bar'], 'right structure';
is $results->array, undef, 'no more results';

# Non-blocking query where not all results have been fetched
my ($fail, $result);
Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $db->query('select name from results_test' => $delay->begin);
  },
  sub {
    my ($delay, $err, $results) = @_;
    $fail = $err;
    push @$result, $results->array;
    $results->finish;
    $db->query('select name from results_test' => $delay->begin);
  },
  sub {
    my ($delay, $err, $results) = @_;
    $fail ||= $err;
    push @$result, $results->array;
    $results->finish;
    $db->query('select name from results_test' => $delay->begin);
  },
  sub {
    my ($delay, $err, $results) = @_;
    $fail ||= $err;
    push @$result, $results->array;
  }
)->wait;
ok !$fail, 'no error';
is_deeply $result, [['foo'], ['foo'], ['foo']], 'right structure';

# Transactions
{
  my $tx = $db->begin;
  $db->query("insert into results_test (name) values ('tx1')");
  $db->query("insert into results_test (name) values ('tx1')");
  $tx->commit;
};
is_deeply $db->query('select * from results_test where name = ?', 'tx1')
  ->hashes->to_array, [{id => 3, name => 'tx1'}, {id => 4, name => 'tx1'}],
  'right structure';
{
  my $tx = $db->begin;
  $db->query("insert into results_test (name) values ('tx2')");
  $db->query("insert into results_test (name) values ('tx2')");
};
is_deeply $db->query('select * from results_test where name = ?', 'tx2')
  ->hashes->to_array, [], 'no results';
eval {
  my $tx = $db->begin;
  $db->query("insert into results_test (name) values ('tx3')");
  $db->query("insert into results_test (name) values ('tx3')");
  $db->query('does_not_exist');
  $tx->commit;
};
like $@, qr/does_not_exist/, 'right error';
is_deeply $db->query('select * from results_test where name = ?', 'tx3')
  ->hashes->to_array, [], 'no results';

# Long-lived results
my $results1 = $db->query('select 1 as one');
is_deeply $results1->hashes, [{one => 1}], 'right structure';
my $results2 = $db->query('select 1 as one');
undef $results1;
is_deeply $results2->hashes, [{one => 1}], 'right structure';

# Clean up once we are done
$pg->db->query('drop schema mojo_results_test cascade');

done_testing();
