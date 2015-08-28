use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use Mojo::IOLoop;
use Mojo::Pg;

# Notifications with event loop
my $pg = Mojo::Pg->new($ENV{TEST_ONLINE});
my ($db, @all, @test);
$pg->pubsub->on(reconnect => sub { $db = pop });
$pg->pubsub->listen(
  pstest => sub {
    my ($pubsub, $payload) = @_;
    push @test, $payload;
    Mojo::IOLoop->next_tick(sub { $pubsub->pg->db->notify(pstest => 'stop') });
    Mojo::IOLoop->stop if $payload eq 'stop';
  }
);
$db->on(notification => sub { push @all, [@_[1, 3]] });
$pg->db->notify(pstest => 'test');
Mojo::IOLoop->start;
is_deeply \@test, ['test', 'stop'], 'right messages';
is_deeply \@all, [['pstest', 'test'], ['pstest', 'stop']],
  'right notifications';

# Unsubscribe
$pg = Mojo::Pg->new($ENV{TEST_ONLINE});
$db = undef;
$pg->pubsub->on(reconnect => sub { $db = pop });
@all = @test = ();
my $first  = $pg->pubsub->listen(pstest => sub { push @test, pop });
my $second = $pg->pubsub->listen(pstest => sub { push @test, pop });
$db->on(notification => sub { push @all, [@_[1, 3]] });
$pg->pubsub->notify('pstest')->notify(pstest => 'first');
is_deeply \@test, ['', '', 'first', 'first'], 'right messages';
is_deeply \@all, [['pstest', ''], ['pstest', 'first']], 'right notifications';
$pg->pubsub->unlisten(pstest => $first)->notify(pstest => 'second');
is_deeply \@test, ['', '', 'first', 'first', 'second'], 'right messages';
is_deeply \@all, [['pstest', ''], ['pstest', 'first'], ['pstest', 'second']],
  'right notifications';
$pg->pubsub->unlisten(pstest => $second)->notify(pstest => 'third');
is_deeply \@test, ['', '', 'first', 'first', 'second'], 'right messages';
is_deeply \@all, [['pstest', ''], ['pstest', 'first'], ['pstest', 'second']],
  'right notifications';

# Reconnect while listening
$pg = Mojo::Pg->new($ENV{TEST_ONLINE});
my @dbhs = @test = ();
$pg->pubsub->on(reconnect => sub { push @dbhs, pop->dbh });
$pg->pubsub->listen(pstest => sub { push @test, pop });
ok $dbhs[0], 'database handle';
is_deeply \@test, [], 'no messages';
{
  local $dbhs[0]{Warn} = 0;
  $pg->pubsub->on(
    reconnect => sub { shift->notify(pstest => 'works'); Mojo::IOLoop->stop });
  $pg->db->query('select pg_terminate_backend(?)', $dbhs[0]{pg_pid});
  Mojo::IOLoop->start;
  ok $dbhs[1], 'database handle';
  isnt $dbhs[0], $dbhs[1], 'different database handles';
  is_deeply \@test, ['works'], 'right messages';
};

# Reconnect while not listening
$pg = Mojo::Pg->new($ENV{TEST_ONLINE});
@dbhs = @test = ();
$pg->pubsub->on(reconnect => sub { push @dbhs, pop->dbh });
$pg->pubsub->notify(pstest => 'fail');
ok $dbhs[0], 'database handle';
is_deeply \@test, [], 'no messages';
{
  local $dbhs[0]{Warn} = 0;
  $pg->pubsub->on(reconnect => sub { Mojo::IOLoop->stop });
  $pg->db->query('select pg_terminate_backend(?)', $dbhs[0]{pg_pid});
  Mojo::IOLoop->start;
  ok $dbhs[1], 'database handle';
  isnt $dbhs[0], $dbhs[1], 'different database handles';
  $pg->pubsub->listen(pstest => sub { push @test, pop });
  $pg->pubsub->notify(pstest => 'works too');
  is_deeply \@test, ['works too'], 'right messages';
};

# Fork-safety
$pg = Mojo::Pg->new($ENV{TEST_ONLINE});
@dbhs = @test = ();
$pg->pubsub->on(reconnect => sub { push @dbhs, pop->dbh });
$pg->pubsub->listen(pstest => sub { push @test, pop });
ok $dbhs[0], 'database handle';
ok $dbhs[0]->ping, 'connected';
$pg->pubsub->notify(pstest => 'first');
is_deeply \@test, ['first'], 'right messages';
{
  local $$ = -23;
  $pg->pubsub->notify(pstest => 'second');
  ok $dbhs[1], 'database handle';
  ok $dbhs[1]->ping, 'connected';
  isnt $dbhs[0], $dbhs[1], 'different database handles';
  ok !$dbhs[0]->ping, 'not connected';
  is_deeply \@test, ['first'], 'right messages';
  $pg->pubsub->listen(pstest => sub { push @test, pop });
  $pg->pubsub->notify(pstest => 'third');
  ok $dbhs[1]->ping, 'connected';
  ok !$dbhs[2], 'no database handle';
  is_deeply \@test, ['first', 'third'], 'right messages';
};

done_testing();
