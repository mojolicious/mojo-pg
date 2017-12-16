use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use Mojo::IOLoop;
use Mojo::JSON 'true';
use Mojo::Pg;
use Scalar::Util 'refaddr';

# Connected
my $pg = Mojo::Pg->new($ENV{TEST_ONLINE});
ok $pg->db->ping, 'connected';

# Custom search_path
$pg = Mojo::Pg->new($ENV{TEST_ONLINE})->search_path(['$user', 'foo', 'bar']);
is_deeply $pg->db->query('show search_path')->hash,
  {search_path => '"$user", foo, bar'}, 'right structure';
$pg = Mojo::Pg->new($ENV{TEST_ONLINE});

# Blocking select
is_deeply $pg->db->query('select 1 as one, 2 as two, 3 as three')->hash,
  {one => 1, two => 2, three => 3}, 'right structure';

# Non-blocking select
my ($fail, $result);
my $same;
my $db = $pg->db;
$db->query(
  'select 1 as one, 2 as two, 3 as three' => sub {
    my ($db, $err, $results) = @_;
    $fail   = $err;
    $result = $results->hash;
    $same   = $db->dbh eq $results->db->dbh;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok $same, 'same database handles';
ok !$fail, 'no error';
is_deeply $result, {one => 1, two => 2, three => 3}, 'right structure';

# Concurrent non-blocking selects
($fail, $result) = ();
Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $pg->db->query('select 1 as one' => $delay->begin);
    $pg->db->query('select 2 as two' => $delay->begin);
    $pg->db->query('select 2 as two' => $delay->begin);
  },
  sub {
    my ($delay, $err_one, $one, $err_two, $two, $err_again, $again) = @_;
    $fail = $err_one || $err_two || $err_again;
    $result = [$one->hashes->first, $two->hashes->first, $again->hashes->first];
  }
)->wait;
ok !$fail, 'no error';
is_deeply $result, [{one => 1}, {two => 2}, {two => 2}], 'right structure';

# Sequential non-blocking selects
($fail, $result) = (undef, []);
$db = $pg->db;
Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $db->query('select 1 as one' => $delay->begin);
  },
  sub {
    my ($delay, $err, $one) = @_;
    $fail = $err;
    push @$result, $one->hashes->first;
    $db->query('select 1 as one' => $delay->begin);
  },
  sub {
    my ($delay, $err, $again) = @_;
    $fail ||= $err;
    push @$result, $again->hashes->first;
    $db->query('select 2 as two' => $delay->begin);
  },
  sub {
    my ($delay, $err, $two) = @_;
    $fail ||= $err;
    push @$result, $two->hashes->first;
  }
)->wait;
ok !$fail, 'no error';
is_deeply $result, [{one => 1}, {one => 1}, {two => 2}], 'right structure';

# Connection cache
is $pg->max_connections, 1, 'right default';
$pg->max_connections(5);
my @dbhs = map { refaddr $_->dbh } $pg->db, $pg->db, $pg->db, $pg->db, $pg->db;
is_deeply \@dbhs,
  [reverse map { refaddr $_->dbh } $pg->db, $pg->db, $pg->db, $pg->db, $pg->db],
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
my $sth = $db->query('select 3 as three')->sth;
is $db->query('select 3 as three')->sth,  $sth, 'same statement handle';
isnt $db->query('select 4 as four')->sth, $sth, 'different statement handles';
is $db->query('select 3 as three')->sth,  $sth, 'same statement handle';
undef $db;
$db = $pg->db;
my $results = $db->query('select 3 as three');
is $results->sth, $sth, 'same statement handle';
isnt $db->query('select 3 as three')->sth, $sth, 'different statement handles';
$sth = $db->query('select 3 as three')->sth;
is $db->query('select 3 as three')->sth,  $sth, 'same statement handle';
isnt $db->query('select 5 as five')->sth, $sth, 'different statement handles';
isnt $db->query('select 6 as six')->sth,  $sth, 'different statement handles';
is $db->query('select 3 as three')->sth,  $sth, 'same statement handle';

# Connection reuse
$db      = $pg->db;
$dbh     = $db->dbh;
$results = $db->query('select 1');
undef $db;
my $db2 = $pg->db;
isnt $db2->dbh, $dbh, 'new database handle';
undef $results;
my $db3 = $pg->db;
is $db3->dbh, $dbh, 'same database handle';
$results = $db3->query('select 2');
is $results->db->dbh, $dbh, 'same database handle';
is $results->array->[0], 2, 'right result';

# Dollar only
$db = $pg->db;
is $db->dollar_only->query('select $1::int as test', 23)->hash->{test}, 23,
  'right result';
eval { $db->dollar_only->query('select ?::int as test', 23) };
like $@, qr/Statement has no placeholders to bind/, 'right error';
is $db->query('select ?::int as test', 23)->hash->{test}, 23, 'right result';

# JSON
$db = $pg->db;
is_deeply $db->query('select ?::json as foo', {json => {bar => 'baz'}})
  ->expand->hash, {foo => {bar => 'baz'}}, 'right structure';
is_deeply $db->query('select ?::jsonb as foo', {json => {bar => 'baz'}})
  ->expand->hash, {foo => {bar => 'baz'}}, 'right structure';
is_deeply $db->query('select ?::json as foo', {json => {bar => 'baz'}})
  ->expand->array, [{bar => 'baz'}], 'right structure';
is_deeply $db->query('select ?::json as foo', {json => {bar => 'baz'}})
  ->expand->hashes->first, {foo => {bar => 'baz'}}, 'right structure';
is_deeply $db->query('select ?::json as foo', {json => {bar => 'baz'}})
  ->expand->arrays->first, [{bar => 'baz'}], 'right structure';
is_deeply $db->query('select ?::json as foo', {json => {bar => 'baz'}})->hash,
  {foo => '{"bar":"baz"}'}, 'right structure';
is_deeply $db->query('select ?::json as foo', {json => \1})
  ->expand->hashes->first, {foo => true}, 'right structure';
is_deeply $db->query('select ?::json as foo', undef)->expand->hash,
  {foo => undef}, 'right structure';
is_deeply $db->query('select ?::json as foo', undef)->expand->array, [undef],
  'right structure';
$results = $db->query('select ?::json', undef);
is_deeply $results->expand->array, [undef], 'right structure';
is_deeply $results->expand->array, undef, 'no more results';
is_deeply $db->query('select ?::json as unicode', {json => {'☃' => '♥'}})
  ->expand->hash, {unicode => {'☃' => '♥'}}, 'right structure';
is_deeply $db->query("select json_build_object('☃', ?::text) as unicode",
  '♥')->expand->hash, {unicode => {'☃' => '♥'}}, 'right structure';

# Fork-safety
$dbh = $pg->db->dbh;
my ($connections, $current) = @_;
$pg->on(
  connection => sub {
    my ($pg, $dbh) = @_;
    $connections++;
    $current = $dbh;
  }
);
is $pg->db->dbh, $dbh, 'same database handle';
ok !$connections, 'no new connections';
{
  local $$ = -23;
  isnt $pg->db->dbh, $dbh,     'different database handles';
  is $pg->db->dbh,   $current, 'same database handle';
  is $connections, 1, 'one new connection';
};
$pg->unsubscribe('connection');

# Shared connection cache
my $pg2 = Mojo::Pg->new($pg);
is $pg2->parent, $pg, 'right parent';
$dbh = $pg->db->dbh;
is $pg->db->dbh,  $dbh, 'same database handle';
is $pg2->db->dbh, $dbh, 'same database handle';
is $pg->db->dbh,  $dbh, 'same database handle';
is $pg2->db->dbh, $dbh, 'same database handle';
$db = $pg->db;
is_deeply $db->query('select 1 as one')->hashes->to_array, [{one => 1}],
  'right structure';
$dbh = $db->dbh;
$db->disconnect;
$db = $pg2->db;
is_deeply $db->query('select 1 as one')->hashes->to_array, [{one => 1}],
  'right structure';
isnt $db->dbh, $dbh, 'different database handle';

# Notifications
$db = $pg->db;
ok !$db->is_listening, 'not listening';
ok $db->listen('dbtest')->is_listening, 'listening';
$db2 = $pg->db->listen('dbtest');
my @notifications;
Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $db->once(notification => $delay->begin);
    $db2->once(notification => $delay->begin);
    Mojo::IOLoop->next_tick(sub { $db2->notify(dbtest => 'foo') });
  },
  sub {
    my ($delay, $name, $pid, $payload, $name2, $pid2, $payload2) = @_;
    push @notifications, [$name, $pid, $payload], [$name2, $pid2, $payload2];
    $db->once(notification => $delay->begin);
    $db2->unlisten('dbtest');
    Mojo::IOLoop->next_tick(sub { $pg->db->notify('dbtest') });
  },
  sub {
    my ($delay, $name, $pid, $payload) = @_;
    push @notifications, [$name, $pid, $payload];
    $db2->listen('dbtest2')->once(notification => $delay->begin);
    Mojo::IOLoop->next_tick(sub { $db2->query("notify dbtest2, 'bar'") });
  },
  sub {
    my ($delay, $name, $pid, $payload) = @_;
    push @notifications, [$name, $pid, $payload];
    $db2->once(notification => $delay->begin);
    my $tx = $db2->begin;
    Mojo::IOLoop->next_tick(
      sub {
        $db2->notify(dbtest2 => 'baz');
        $tx->commit;
      }
    );
  },
  sub {
    my ($delay, $name, $pid, $payload) = @_;
    push @notifications, [$name, $pid, $payload];
  }
)->wait;
ok !$db->unlisten('dbtest')->is_listening, 'not listening';
ok !$db2->unlisten('*')->is_listening,     'not listening';
is $notifications[0][0], 'dbtest',  'right channel name';
ok $notifications[0][1], 'has process id';
is $notifications[0][2], 'foo',     'right payload';
is $notifications[1][0], 'dbtest',  'right channel name';
ok $notifications[1][1], 'has process id';
is $notifications[1][2], 'foo',     'right payload';
is $notifications[2][0], 'dbtest',  'right channel name';
ok $notifications[2][1], 'has process id';
is $notifications[2][2], '',        'no payload';
is $notifications[3][0], 'dbtest2', 'right channel name';
ok $notifications[3][1], 'has process id';
is $notifications[3][2], 'bar',     'no payload';
is $notifications[4][0], 'dbtest2', 'right channel name';
ok $notifications[4][1], 'has process id';
is $notifications[4][2], 'baz',     'no payload';
is $notifications[5], undef, 'no more notifications';

# Stop listening for all notifications
ok !$db->is_listening, 'not listening';
ok $db->listen('dbtest')->listen('dbtest2')->unlisten('dbtest2')->is_listening,
  'listening';
ok !$db->unlisten('*')->is_listening, 'not listening';

# Connection close while listening for notifications
{
  ok $db->listen('dbtest')->is_listening, 'listening';
  my $close = 0;
  $db->on(close => sub { $close++ });
  local $db->dbh->{Warn} = 0;
  $pg->db->query('select pg_terminate_backend(?)', $db->pid);
  Mojo::IOLoop->start;
  is $close, 1, 'close event has been emitted once';
};

# Blocking error
eval { $pg->db->query('does_not_exist') };
like $@, qr/does_not_exist.*database\.t/s, 'right error';

# Non-blocking error
($fail, $result) = ();
$pg->db->query(
  'does_not_exist' => sub {
    my ($db, $err, $results) = @_;
    ($fail, $result) = ($err, $results);
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
like $fail, qr/does_not_exist/, 'right error';
is $result->sth->errstr, $fail, 'same error';

# Non-blocking query in progress
$db = $pg->db;
$db->query('select 1' => sub { });
eval {
  $db->query('select 1' => sub { });
};
like $@, qr/Non-blocking query already in progress/, 'right error';

# CLean up non-blocking query
$fail = undef;
$db   = $pg->db;
$db->query(
  'select 1' => sub {
    my ($db, $err, $results) = @_;
    $fail = $err;
  }
);
$db->disconnect;
undef $db;
is $fail, 'Premature connection close', 'right error';

done_testing();
