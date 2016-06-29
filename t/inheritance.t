package MojoPgTest;
use Mojo::Base 'Mojo::Pg';
has db_class => 'MojoPgTestDatabase';

package MojoPgTestDatabase;
use Mojo::Base 'Mojo::Pg::Database';
has result_class => 'MojoPgTestResults';

package MojoPgTestResults;
use Mojo::Base 'Mojo::Pg::Results';

package test;

use Mojo::Base -strict;
use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

my $pg_class='MojoPgTest';
my $db_class = 'MojoPgTestDatabase';
my $result_class = 'MojoPgTestResults';

# Connected
my $pg = MojoPgTest->new($ENV{TEST_ONLINE})->on_connect(['set datestyle to "DMY, ISO";']);
ok $pg->db->ping, 'connected';
isa_ok $pg, $pg_class, 'top class';

my $db = $pg->db;
isa_ok $db, $db_class, 'database class';

my $result;
my $cb = sub {
  my ($db, $err, $res) = @_;
  die $err if $err;
  $result = $res;
};
$db->query('select ?::date as d', ("30/06/2016"), $cb);
Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
isa_ok $result, $result_class, 'result class';
like $result->hash->{d}, qr/\d{4}-\d{2}-\d{2}/, 'on_connect do datestyle';


my $die = 'OUH, BUHHH!';
my $rc = $db->query('select 1', sub {die $die});
Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
isa_ok $rc, 'Mojo::Reactor::Poll', 'non-blocking callback die';
like $rc->{cb_error}, qr/$die/, 'error on callback through reactor';

done_testing();


