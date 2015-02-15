use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Mojo::IOLoop;
use Mojo::Pg;

# Reconnect
my $pg = Mojo::Pg->new($ENV{TEST_ONLINE});
my (@dbhs, @test);
$pg->pubsub->on(reconnect => sub { push @dbhs, pop->dbh });
ok !$pg->pubsub->db->is_listening, 'not listening';
$pg->pubsub->listen(pstest => sub { push @test, pop });
ok $pg->pubsub->db->is_listening, 'listening';
is $dbhs[0], $pg->pubsub->db->dbh, 'same database handle';
is_deeply \@test, [], 'no messages';
{
  local $pg->pubsub->db->dbh->{Warn} = 0;
  $pg->pubsub->on(reconnect => sub { Mojo::IOLoop->stop });
  $pg->db->query('select pg_terminate_backend(?)', $pg->pubsub->db->pid);
  Mojo::IOLoop->start;
  ok $dbhs[1], 'database handle';
  isnt $dbhs[0], $dbhs[1], 'different database handles';
  $pg->pubsub->notify(pstest => 'works');
  is_deeply \@test, ['works'], 'right messages';
};

# Notifications with event loop
$pg = Mojo::Pg->new($ENV{TEST_ONLINE});
my @all = @test = ();
$pg->pubsub->db->on(notification => sub { push @all, [@_[1, 3]] });
$pg->pubsub->listen(
  pstest => sub {
    my ($pubsub, $payload) = @_;
    push @test, $payload;
    Mojo::IOLoop->next_tick(sub { $pubsub->pg->db->notify(pstest => 'stop') });
    Mojo::IOLoop->stop if $payload eq 'stop';
  }
);
$pg->db->notify(pstest => 'test');
Mojo::IOLoop->start;
is_deeply \@test, ['test', 'stop'], 'right messages';
is_deeply \@all, [['pstest', 'test'], ['pstest', 'stop']],
  'right notifications';

# Unsubscribe
$pg = Mojo::Pg->new($ENV{TEST_ONLINE});
@all = @test = ();
$pg->pubsub->db->on(notification => sub { push @all, [@_[1, 3]] });
my $first  = $pg->pubsub->listen(pstest => sub { push @test, pop });
my $second = $pg->pubsub->listen(pstest => sub { push @test, pop });
$pg->pubsub->notify(pstest => 'first');
is_deeply \@test, ['first', 'first'], 'right messages';
is_deeply \@all, [['pstest', 'first']], 'right notifications';
$pg->pubsub->unlisten(pstest => $first)->notify(pstest => 'second');
is_deeply \@test, ['first', 'first', 'second'], 'right messages';
is_deeply \@all, [['pstest', 'first'], ['pstest', 'second']],
  'right notifications';
ok $pg->pubsub->db->is_listening, 'listening';
$pg->pubsub->unlisten(pstest => $second)->notify(pstest => 'third');
ok !$pg->pubsub->db->is_listening, 'not listening';
is_deeply \@test, ['first', 'first', 'second'], 'right messages';
is_deeply \@all, [['pstest', 'first'], ['pstest', 'second']],
  'right notifications';

done_testing();
