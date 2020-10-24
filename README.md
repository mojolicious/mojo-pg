
# Mojo::Pg [![](https://github.com/mojolicious/mojo-pg/workflows/linux/badge.svg)](https://github.com/mojolicious/mojo-pg/actions)

  A tiny wrapper around [DBD::Pg](https://metacpan.org/pod/DBD::Pg) that makes [PostgreSQL](https://www.postgresql.org)
  a lot of fun to use with the [Mojolicious](https://mojolicious.org) real-time web framework.

```perl
use Mojolicious::Lite -signatures;
use Mojo::Pg;

helper pg => sub { state $pg = Mojo::Pg->new('postgresql://postgres@/test') };

# Use migrations to create a table during startup
app->pg->migrations->from_data->migrate;

get '/' => sub ($c) {

  my $db = $c->pg->db;
  my $ip = $c->tx->remote_address;

  # Store information about current visitor blocking
  $db->query('INSERT INTO visitors VALUES (NOW(), ?)', $ip);

  # Retrieve information about previous visitors non-blocking
  $db->query('SELECT * FROM visitors LIMIT 50' => sub ($db, $err, $results) {

    return $c->reply->exception($err) if $err;

    $c->render(json => $results->hashes->to_array);
  });
};

app->start;
__DATA__

@@ migrations
-- 1 up
CREATE TABLE visitors (at TIMESTAMP WITH TIME ZONE, ip TEXT);
-- 1 down
DROP TABLE visitors;
```

## Installation

  All you need is a one-liner, it takes less than a minute.

    $ curl -L https://cpanmin.us | perl - -M https://cpan.metacpan.org -n Mojo::Pg

  We recommend the use of a [Perlbrew](http://perlbrew.pl) environment.

## Want to know more?

  Take a look at our excellent
  [documentation](https://mojolicious.org/perldoc/Mojo/Pg)!
