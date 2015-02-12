use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Mojo::Pg;

my $pg = Mojo::Pg->new($ENV{TEST_ONLINE});
my $db = $pg->db->do(
  'create table if not exists results_test (
     id   serial primary key,
     name text,
     info json
   )'
);
$db->do('insert into results_test (name, info) values (?, ?)',
  $_, {json => {$_ => $_}})
  for qw(foo bar);

# Result methods
is_deeply $db->query('select * from results_test')->rows, 2, 'two rows';
is_deeply $db->query('select * from results_test')->columns,
  ['id', 'name', 'info'], 'right structure';
is_deeply $db->query('select * from results_test')->array,
  [1, 'foo', '{"foo":"foo"}'], 'right structure';
is_deeply $db->query('select * from results_test')->arrays->to_array,
  [[1, 'foo', '{"foo":"foo"}'], [2, 'bar', '{"bar":"bar"}']],
  'right structure';
is_deeply $db->query('select * from results_test')->hash,
  {id => 1, name => 'foo', info => '{"foo":"foo"}'}, 'right structure';
my $results = [
  {id => 1, name => 'foo', info => {foo => 'foo'}},
  {id => 2, name => 'bar', info => {bar => 'bar'}}
];
is_deeply $db->query('select * from results_test')->expand->hashes->to_array,
  $results, 'right structure';
is $pg->db->query('select * from results_test')->text,
  qq/1  foo  {"foo":"foo"}\n2  bar  {"bar":"bar"}\n/, 'right text';

# Transactions
{
  my $tx = $db->begin;
  $db->do("insert into results_test (name) values ('tx1')")
    ->do("insert into results_test (name) values ('tx1')");
  $tx->commit;
};
$results = [
  {id => 3, name => 'tx1', info => undef},
  {id => 4, name => 'tx1', info => undef}
];
is_deeply $db->query('select * from results_test where name = ?', 'tx1')
  ->hashes->to_array, $results, 'right structure';
{
  my $tx = $db->begin;
  $db->do("insert into results_test (name) values ('tx2')")
    ->do("insert into results_test (name) values ('tx2')");
};
is_deeply $db->query('select * from results_test where name = ?', 'tx2')
  ->hashes->to_array, [], 'no results';
eval {
  my $tx = $db->begin;
  $db->do("insert into results_test (name) values ('tx3')")
    ->do("insert into results_test (name) values ('tx3')")
    ->do('does_not_exist');
  $tx->commit;
};
like $@, qr/does_not_exist/, 'right error';
is_deeply $db->query('select * from results_test where name = ?', 'tx3')
  ->hashes->to_array, [], 'no results';

$db->do('drop table results_test');

done_testing();
