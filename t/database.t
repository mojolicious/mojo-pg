use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_DSN to enable this test' unless $ENV{TEST_DSN};

use Mojo::IOLoop;
use Mojo::Pg;

# Defaults
my $pg = Mojo::Pg->new;
is $pg->dsn,      'dbi:Pg:dbname=test', 'right data source';
is $pg->username, '',                   'no username';
is $pg->password, '',                   'no password';
is_deeply $pg->options, {AutoCommit => 1, PrintError => 0, RaiseError => 1},
  'right options';

# Arguments
$pg = Mojo::Pg->new('dbi:Pg:dbname=test2;host=127.0.0.1');
is $pg->dsn,      'dbi:Pg:dbname=test2;host=127.0.0.1', 'right data source';
is $pg->username, '',                                   'no username';
is $pg->password, '',                                   'no password';
is_deeply $pg->options, {AutoCommit => 1, PrintError => 0, RaiseError => 1},
  'right options';
$pg = Mojo::Pg->new('dbi:Pg:dbname=test2', 'tester', 'testing',
  {PrintError => 1});
is $pg->dsn,      'dbi:Pg:dbname=test2', 'right data source';
is $pg->username, 'tester',              'right username';
is $pg->password, 'testing',             'right password';
is_deeply $pg->options, {PrintError => 1}, 'right options';

$pg = Mojo::Pg->new($ENV{TEST_DSN}, $ENV{TEST_USERNAME}, $ENV{TEST_PASSWORD});
ok $pg->db->ping, 'connected';

# Blocking select
is_deeply $pg->db->query('select 1 as one, 2 as two, 3 as three')->hash,
  {one => 1, two => 2, three => 3}, 'right structure';

# Non-blocking select
my ($fail, $result);
my $db = $pg->db;
is $db->backlog, 0, 'no operations waiting';
$db->query(
  'select 1 as one, 2 as two, 3 as three' => sub {
    my ($db, $err, $results) = @_;
    $fail   = $err;
    $result = $results->hash;
    Mojo::IOLoop->stop;
  }
);
is $db->backlog, 1, 'one operation waiting';
Mojo::IOLoop->start;
is $db->backlog, 0, 'no operations waiting';
ok !$fail, 'no error';
is_deeply $result, {one => 1, two => 2, three => 3}, 'right structure';

# Concurrent non-blocking selects
($fail, $result) = ();
Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    my $db    = $pg->db;
    $db->query('select 1 as one' => $delay->begin);
    $db->query('select 2 as two' => $delay->begin);
    $db->query('select 2 as two' => $delay->begin);
  },
  sub {
    my ($delay, $err_one, $one, $err_two, $two, $err_again, $again) = @_;
    $fail = $err_one || $err_two || $err_again;
    $result
      = [$one->hashes->first, $two->hashes->first, $again->hashes->first];
  }
)->wait;
ok !$fail, 'no error';
is_deeply $result, [{one => 1}, {two => 2}, {two => 2}], 'right structure';

# Connection cache
is $pg->max_connections, 5, 'right default';
my @dbhs = map { $_->dbh } $pg->db, $pg->db, $pg->db, $pg->db, $pg->db;
is_deeply \@dbhs,
  [map { $_->dbh } $pg->db, $pg->db, $pg->db, $pg->db, $pg->db],
  'same database handles';
@dbhs = ();
my $dbh = $pg->max_connections(1)->db->dbh;
is $pg->db->dbh, $dbh, 'same database handle';
isnt $pg->db->dbh, $pg->db->dbh, 'different database handles';
is $pg->db->dbh, $dbh, 'different database handles';
$dbh = $pg->db->dbh;
is $pg->db->dbh, $dbh, 'same database handle';
$pg->db->disconnect;
isnt $pg->db->dbh, $dbh, 'different database handles';

# Statement cache
$db = $pg->db;
is $db->max_statements, 10, 'right default';
my $sth = $db->max_statements(2)->query('select 3 as three')->sth;
is $db->query('select 3 as three')->sth,   $sth, 'same statement handle';
isnt $db->query('select 4 as four')->sth,  $sth, 'different statement handles';
is $db->query('select 3 as three')->sth,   $sth, 'same statement handle';
isnt $db->query('select 5 as five')->sth,  $sth, 'different statement handles';
isnt $db->query('select 6 as six')->sth,   $sth, 'different statement handles';
isnt $db->query('select 3 as three')->sth, $sth, 'different statement handles';

# Fork safety
$dbh = $pg->db->dbh;
{
  local $$ = -23;
  isnt $pg->db->dbh, $dbh, 'different database handles';
};

# Notifications
$db = $pg->db;
ok !$db->is_listening, 'not listening';
$db->listen('foo');
ok $db->is_listening, 'listening';
Mojo::IOLoop->timer(
  0 => sub { $pg->db->query('select pg_notify(?, ?)', 'foo', 'bar') });
my @notification;
$db->once(
  notification => sub {
    my ($db, $name, $pid, $payload) = @_;
    @notification = ($name, $pid, $payload);
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
$db->unlisten('foo');
ok !$db->is_listening, 'not listening';
is $notification[0], 'foo', 'right channel name';
ok $notification[1], 'has process id';
is $notification[2], 'bar', 'right payload';

# Stop listening for all notifications
ok !$db->is_listening, 'not listening';
ok $db->listen('foo')->listen('bar')->unlisten('bar')->is_listening,
  'listening';
ok !$db->unlisten('*')->is_listening, 'not listening';

# Blocking error
eval { $pg->db->query('does_not_exist') };
like $@, qr/does_not_exist/, 'right error';

# Non-blocking error
($fail, $result) = ();
$pg->db->query(
  'does_not_exist' => sub {
    my ($db, $err, $results) = @_;
    $fail   = $err;
    $result = $results;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
like $fail, qr/does_not_exist/, 'right error';
is $result, undef, 'no result';

done_testing();
