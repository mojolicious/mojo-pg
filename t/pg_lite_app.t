use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Mojo::Pg;
use Mojolicious::Lite;
use Test::Mojo;

helper pg => sub { state $pg = Mojo::Pg->new($ENV{TEST_ONLINE}) };

app->pg->db->do('create table if not exists app_test (stuff varchar(255))')
  ->do("insert into app_test values ('I ♥ Mojolicious!')");

get '/blocking' => sub {
  my $c = shift;
  $c->render(
    text => $c->pg->db->query('select * from app_test')->hash->{stuff});
};

get '/non-blocking' => sub {
  my $c = shift;
  $c->pg->db->query(
    'select * from app_test' => sub {
      my ($db, $err, $results) = @_;
      $c->render(text => $results->hash->{stuff});
    }
  );
};

my $t = Test::Mojo->new;

# Blocking select
$t->get_ok('/blocking')->status_is(200)->content_is('I ♥ Mojolicious!');

# Non-blocking select
$t->get_ok('/non-blocking')->status_is(200)->content_is('I ♥ Mojolicious!');
$t->app->pg->db->do('drop table app_test');

done_testing();
