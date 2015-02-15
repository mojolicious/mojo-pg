use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Mojo::IOLoop;
use Mojo::Pg;

# Notifications with event loop
my $pg = Mojo::Pg->new($ENV{TEST_ONLINE});
my (@all, @test);
$pg->pubsub->db->on(notification => sub { push @all, [@_[1, 3]] });
$pg->pubsub->listen(
  pstest => sub {
    my ($pubsub, $payload) = @_;
    push @test, $payload;
    Mojo::IOLoop->next_tick(sub { $pubsub->notify(pstest => 'stop') });
    Mojo::IOLoop->stop if $payload eq 'stop';
  }
);
Mojo::IOLoop->next_tick(sub { $pg->db->notify(pstest => 'test') });
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
