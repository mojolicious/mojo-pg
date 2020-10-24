use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use Mojo::Pg;
use Mojolicious::Lite;
use Scalar::Util qw(refaddr);
use Test::Mojo;

# Isolate tests
my $pg = Mojo::Pg->new($ENV{TEST_ONLINE});
$pg->db->query('DROP SCHEMA IF EXISTS mojo_app_test CASCADE');
$pg->db->query('CREATE SCHEMA mojo_app_test');

helper pg => sub {
  state $pg = Mojo::Pg->new($ENV{TEST_ONLINE})->search_path(['mojo_app_test']);
};

app->pg->db->query('CREATE TABLE IF NOT EXISTS app_test (stuff TEXT)');
app->pg->db->query('INSERT INTO app_test VALUES (?)', 'I ♥ Mojolicious!');

get '/blocking' => sub {
  my $c  = shift;
  my $db = $c->pg->db;
  $c->res->headers->header('X-Ref' => refaddr $db->dbh);
  $c->render(text => $db->query('SELECT * FROM app_test')->hash->{stuff});
};

get '/non-blocking' => sub {
  my $c = shift;
  $c->pg->db->query(
    'SELECT * FROM app_test' => sub {
      my ($db, $err, $results) = @_;
      $c->res->headers->header('X-Ref' => refaddr $db->dbh);
      $c->render(text => $results->hash->{stuff});
    }
  );
};

my $t = Test::Mojo->new;

subtest 'Make sure migrations are not served as static files' => sub {
  $t->get_ok('/app_test')->status_is(404);
};

subtest 'Blocking select (with connection reuse)' => sub {
  $t->get_ok('/blocking')->status_is(200)->content_is('I ♥ Mojolicious!');
  my $ref = $t->tx->res->headers->header('X-Ref');
  $t->get_ok('/blocking')->status_is(200)->header_is('X-Ref', $ref)->content_is('I ♥ Mojolicious!');
  $t->get_ok('/blocking')->status_is(200)->header_is('X-Ref', $ref)->content_is('I ♥ Mojolicious!');
};

subtest 'Non-blocking select (with connection reuse)' => sub {
  $t->get_ok('/non-blocking')->status_is(200)->content_is('I ♥ Mojolicious!');
  my $ref = $t->tx->res->headers->header('X-Ref');
  $t->get_ok('/non-blocking')->status_is(200)->header_is('X-Ref', $ref)->content_is('I ♥ Mojolicious!');
  $t->get_ok('/non-blocking')->status_is(200)->header_is('X-Ref', $ref)->content_is('I ♥ Mojolicious!');
};

# Clean up once we are done
$pg->db->query('DROP SCHEMA mojo_app_test CASCADE');

done_testing();
