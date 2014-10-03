
# Mojo::Pg [![Build Status](https://travis-ci.org/kraih/mojo-pg.svg?branch=master)](https://travis-ci.org/kraih/mojo-pg)

  A tiny wrapper around [DBD::Pg](https://metacpan.org/pod/DBD::Pg) that makes
  [PostgreSQL](http://www.postgresql.org) more fun to use with the
  [Mojolicious](http://mojolicio.us) real-time web framework.

```perl
use Mojolicious::Lite;
use Mojo::Pg;

helper pg =>
  sub { state $pg = Mojo::Pg->new('dbi:Pg:dbname=test', 'postgres') };

# Prepare a table during startup
app->pg->db->do(
  'create table if not exists visitors (
     at timestamp,
     ip varchar(255)
   )'
);

get '/' => sub {
  my $c = shift;

  my $db = $c->pg->db;
  my $ip = $c->tx->remote_address;

  # Store information about current visitor blocking
  $db->query('insert into visitors values (now(), ?)', $ip);

  # Retrieve information about previous visitors non-blocking
  $db->query('select * from visitors limit 50' => sub {
    my ($db, $err, $results) = @_;

    return $c->reply->exception($err) if $err;

    $c->render(json => [$results->hashes->each]);
  });
};

app->start;
```

## Installation

  All you need is a oneliner, it takes less than a minute.

    $ curl -L cpanmin.us | perl - -n Mojo::Pg

  We recommend the use of a [Perlbrew](http://perlbrew.pl) environment.
