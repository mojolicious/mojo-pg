use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use Mojo::IOLoop;
use Mojo::JSON qw(true);
use Mojo::Pg;
use Mojo::Promise;
use Scalar::Util qw(refaddr);

my $pg = Mojo::Pg->new($ENV{TEST_ONLINE});

subtest 'Connected' => sub {
  ok $pg->db->ping, 'connected';
};

subtest 'Custom search_path' => sub {
  $pg = Mojo::Pg->new($ENV{TEST_ONLINE})->search_path(['$user', 'foo', 'bar']);
  is_deeply $pg->db->query('SHOW search_path')->hash, {search_path => '"$user", foo, bar'}, 'right structure';
  $pg = Mojo::Pg->new($ENV{TEST_ONLINE});
};

subtest 'Blocking select' => sub {
  is_deeply $pg->db->query('SELECT 1 AS one, 2 AS two, 3 AS three')->hash, {one => 1, two => 2, three => 3},
    'right structure';
};

subtest 'Non-blocking select' => sub {
  my ($fail, $result);
  my $same;
  my $db = $pg->db;
  $db->query(
    'SELECT 1 AS one, 2 AS two, 3 AS three' => sub {
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
};

subtest 'Concurrent non-blocking selects' => sub {
  my ($fail, $result);
  Mojo::Promise->all(
    $pg->db->query_p('SELECT 1 AS one'),
    $pg->db->query_p('SELECT 2 AS two'),
    $pg->db->query_p('SELECT 2 AS two')
  )->then(sub {
    my ($one, $two, $three) = @_;
    $result = [$one->[0]->hashes->first, $two->[0]->hashes->first, $three->[0]->hashes->first];
  })->catch(sub { $fail = shift })->wait;
  ok !$fail, 'no error';
  is_deeply $result, [{one => 1}, {two => 2}, {two => 2}], 'right structure';
};

subtest 'Sequential non-blocking selects' => sub {
  my ($fail, $result);
  my $db = $pg->db;
  $db->query_p('SELECT 1 AS one')->then(sub {
    push @$result, shift->hashes->first;
    return $db->query_p('SELECT 1 AS one');
  })->then(sub {
    push @$result, shift->hashes->first;
    return $db->query_p('SELECT 2 AS two');
  })->then(sub {
    push @$result, shift->hashes->first;
  })->catch(sub { $fail = shift })->wait;
  ok !$fail, 'no error';
  is_deeply $result, [{one => 1}, {one => 1}, {two => 2}], 'right structure';
};

subtest 'Connection cache' => sub {
  is $pg->max_connections, 1, 'right default';
  $pg->max_connections(5);
  my @dbhs = map { refaddr $_->dbh } $pg->db, $pg->db, $pg->db, $pg->db, $pg->db;
  is_deeply \@dbhs, [reverse map { refaddr $_->dbh } $pg->db, $pg->db, $pg->db, $pg->db, $pg->db],
    'same database handles';
  @dbhs = ();
  my $dbh = $pg->max_connections(1)->db->dbh;
  is $pg->db->dbh,   $dbh, 'same database handle';
  isnt $pg->db->dbh, $pg->db->dbh, 'different database handles';
  is $pg->db->dbh,   $dbh, 'different database handles';
  $dbh = $pg->db->dbh;
  is $pg->db->dbh, $dbh, 'same database handle';
  $pg->db->disconnect;
  isnt $pg->db->dbh, $dbh, 'different database handles';
};

subtest 'Statement cache' => sub {
  my $db  = $pg->db;
  my $sth = $db->query('SELECT 3 AS three')->sth;
  is $db->query('SELECT 3 AS three')->sth,  $sth, 'same statement handle';
  isnt $db->query('SELECT 4 AS four')->sth, $sth, 'different statement handles';
  is $db->query('SELECT 3 AS three')->sth,  $sth, 'same statement handle';
  undef $db;
  $db = $pg->db;
  my $results = $db->query('SELECT 3 AS three');
  is $results->sth, $sth, 'same statement handle';
  isnt $db->query('SELECT 3 AS three')->sth, $sth, 'different statement handles';
  $sth = $db->query('SELECT 3 AS three')->sth;
  is $db->query('SELECT 3 AS three')->sth,  $sth, 'same statement handle';
  isnt $db->query('SELECT 5 AS five')->sth, $sth, 'different statement handles';
  isnt $db->query('SELECT 6 AS six')->sth,  $sth, 'different statement handles';
  is $db->query('SELECT 3 AS three')->sth,  $sth, 'same statement handle';
};

subtest 'Connection reuse' => sub {
  my $db      = $pg->db;
  my $dbh     = $db->dbh;
  my $results = $db->query('select 1');
  undef $db;
  my $db2 = $pg->db;
  isnt $db2->dbh, $dbh, 'new database handle';
  undef $results;
  my $db3 = $pg->db;
  is $db3->dbh, $dbh, 'same database handle';
  $results = $db3->query('SELECT 2');
  is $results->db->dbh, $dbh, 'same database handle';
  is $results->array->[0], 2, 'right result';
};

subtest 'Dollar only' => sub {
  my $db = $pg->db;
  is $db->dollar_only->query('SELECT $1::INT AS test', 23)->hash->{test}, 23, 'right result';
  eval { $db->dollar_only->query('SELECT ?::INT AS test', 23) };
  like $@, qr/Statement has no placeholders to bind/, 'right error';
  is $db->query('SELECT ?::INT AS test', 23)->hash->{test}, 23, 'right result';
};

subtest 'JSON' => sub {
  my $db = $pg->db;
  is_deeply $db->query('SELECT ?::JSON AS foo', {json => {bar => 'baz'}})->expand->hash, {foo => {bar => 'baz'}},
    'right structure';
  is_deeply $db->query('SELECT ?::JSONB AS foo', {json => {bar => 'baz'}})->expand->hash, {foo => {bar => 'baz'}},
    'right structure';
  is_deeply $db->query('SELECT ?::JSON AS foo', {json => {bar => 'baz'}})->expand->array, [{bar => 'baz'}],
    'right structure';
  is_deeply $db->query('SELECT ?::JSON AS foo', {json => {bar => 'baz'}})->expand->hashes->first,
    {foo => {bar => 'baz'}}, 'right structure';
  is_deeply $db->query('SELECT ?::JSON AS foo', {json => {bar => 'baz'}})->expand->arrays->first, [{bar => 'baz'}],
    'right structure';
  is_deeply $db->query('SELECT ?::JSON AS foo', {json => {bar => 'baz'}})->hash, {foo => '{"bar":"baz"}'},
    'right structure';
  is_deeply $db->query('SELECT ?::JSON AS foo', {json => \1})->expand->hashes->first, {foo => true}, 'right structure';
  is_deeply $db->query('SELECT ?::JSON AS foo', undef)->expand->hash, {foo => undef}, 'right structure';
  is_deeply $db->query('SELECT ?::JSON AS foo', undef)->expand->array, [undef], 'right structure';
  my $results = $db->query('SELECT ?::json', undef);
  is_deeply $results->expand->array, [undef], 'right structure';
  is_deeply $results->expand->array, undef, 'no more results';
  is_deeply $db->query('SELECT ?::JSON AS unicode', {json => {'☃' => '♥'}})->expand->hash, {unicode => {'☃' => '♥'}},
    'right structure';
  is_deeply $db->query("SELECT JSON_BUILD_OBJECT('☃', ?::TEXT) AS unicode", '♥')->expand->hash,
    {unicode => {'☃' => '♥'}}, 'right structure';
};

subtest 'Fork-safety' => sub {
  my $dbh = $pg->db->dbh;
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
    my $dbh2 = $pg->db->dbh;
    isnt $dbh2,      $dbh,     'different database handles';
    is $dbh2,        $current, 'same database handle';
    is $connections, 1, 'one new connection';
    {
      local $$ = -24;
      isnt $pg->db->dbh, $dbh,     'different database handles';
      isnt $pg->db->dbh, $dbh2,    'different database handles';
      is $pg->db->dbh,   $current, 'same database handle';
      is $connections, 2, 'two new connections';
    };
  };
  $pg->unsubscribe('connection');
};

subtest 'Shared connection cache' => sub {
  my $pg2 = Mojo::Pg->new($pg);
  is $pg2->parent, $pg, 'right parent';
  my $dbh = $pg->db->dbh;
  is $pg->db->dbh,  $dbh, 'same database handle';
  is $pg2->db->dbh, $dbh, 'same database handle';
  is $pg->db->dbh,  $dbh, 'same database handle';
  is $pg2->db->dbh, $dbh, 'same database handle';
  my $db = $pg->db;
  is_deeply $db->query('SELECT 1 AS one')->hashes->to_array, [{one => 1}], 'right structure';
  $dbh = $db->dbh;
  $db->disconnect;
  $db = $pg2->db;
  is_deeply $db->query('SELECT 1 AS one')->hashes->to_array, [{one => 1}], 'right structure';
  isnt $db->dbh, $dbh, 'different database handle';
};

subtest 'Cache reset' => sub {
  my $dbh = $pg->db->dbh;
  is $pg->db->dbh, $dbh, 'same database handle';
  is $pg->db->dbh, $dbh, 'same database handle again';
  is $pg->db->dbh, $dbh, 'same database handle again';
  isnt $pg->reset->db->dbh, $dbh, 'different database handle';
  $dbh = $pg->db->dbh;
  is $pg->db->dbh, $dbh, 'same database handle';
  is $pg->db->dbh, $dbh, 'same database handle again';
  isnt $pg->reset->db->dbh, $dbh, 'different database handle';
};

subtest 'Notifications' => sub {
  my $db = $pg->db;
  ok !$db->is_listening, 'not listening';
  ok $db->listen('dbtest')->is_listening, 'listening';
  my $db2 = $pg->db->listen('dbtest');

  my @result;
  my $promise = Mojo::Promise->new;
  $db->once(notification => sub { shift; $promise->resolve(@_) });
  my $promise2 = Mojo::Promise->new;
  $db2->once(notification => sub { shift; $promise2->resolve(@_) });
  Mojo::IOLoop->next_tick(sub { $db2->notify(dbtest => 'foo') });
  Mojo::Promise->all($promise, $promise2)->then(sub {
    my ($one, $two) = @_;
    push @result, $one, $two;
  })->wait;
  is $result[0][0], 'dbtest', 'right channel name';
  ok $result[0][1], 'has process id';
  is $result[0][2], 'foo',    'right payload';
  is $result[1][0], 'dbtest', 'right channel name';
  ok $result[1][1], 'has process id';
  is $result[1][2], 'foo', 'right payload';

  @result  = ();
  $promise = Mojo::Promise->new;
  $db->once(notification => sub { shift; $promise->resolve(@_) });
  Mojo::IOLoop->next_tick(sub { $pg->db->notify('dbtest') });
  $promise->then(sub { push @result, [@_] })->wait;
  is $result[0][0], 'dbtest', 'right channel name';
  ok $result[0][1], 'has process id';
  is $result[0][2], '', 'no payload';

  @result  = ();
  $promise = Mojo::Promise->new;
  $db2->listen('dbtest2')->once(notification => sub { shift; $promise->resolve(@_) });
  Mojo::IOLoop->next_tick(sub { $db2->query("NOTIFY dbtest2, 'bar'") });
  $promise->then(sub { push @result, [@_] })->wait;
  is $result[0][0], 'dbtest2', 'right channel name';
  ok $result[0][1], 'has process id';
  is $result[0][2], 'bar', 'no payload';

  @result  = ();
  $promise = Mojo::Promise->new;
  $db2->once(notification => sub { shift; $promise->resolve(@_) });
  my $tx = $db2->begin;
  Mojo::IOLoop->next_tick(sub {
    $db2->notify(dbtest2 => 'baz');
    $tx->commit;
  });
  $promise->then(sub { push @result, [@_] })->wait;
  is $result[0][0], 'dbtest2', 'right channel name';
  ok $result[0][1], 'has process id';
  is $result[0][2], 'baz', 'no payload';

  ok !$db->unlisten('dbtest')->is_listening, 'not listening';
  ok !$db2->unlisten('*')->is_listening,     'not listening';
};

subtest 'Stop listening for all notifications' => sub {
  my $db = $pg->db;
  ok !$db->is_listening, 'not listening';
  ok $db->listen('dbtest')->listen('dbtest2')->unlisten('dbtest2')->is_listening, 'listening';
  ok !$db->unlisten('*')->is_listening, 'not listening';
};

subtest 'Connection close while listening for notifications' => sub {
  my $db = $pg->db;
  ok $db->listen('dbtest')->is_listening, 'listening';
  my $close = 0;
  $db->on(close => sub { $close++ });
  local $db->dbh->{Warn} = 0;
  $pg->db->query('SELECT PG_TERMINATE_BACKEND(?)', $db->pid);
  Mojo::IOLoop->start;
  is $close, 1, 'close event has been emitted once';
};

subtest 'Blocking error' => sub {
  eval { $pg->db->query('does_not_exist') };
  like $@, qr/does_not_exist.*database\.t/s, 'right error';
};

subtest 'Non-blocking error' => sub {
  my ($fail, $result);
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
};

subtest 'Non-blocking query in progress' => sub {
  my $db = $pg->db;
  $db->query('SELECT 1' => sub { });
  eval {
    $db->query('SELECT 1' => sub { });
  };
  like $@, qr/Non-blocking query already in progress/, 'right error';
};

subtest 'CLean up non-blocking query' => sub {
  my $fail;
  my $db = $pg->db;
  $db->query(
    'SELECT 1' => sub {
      my ($db, $err, $results) = @_;
      $fail = $err;
    }
  );
  $db->disconnect;
  undef $db;
  is $fail, 'Premature connection close', 'right error';
};

done_testing();
