package Mojo::Pg;
use Mojo::Base 'Mojo::EventEmitter';

use Carp 'croak';
use DBI;
use Mojo::Pg::Database;
use Mojo::Pg::Migrations;
use Mojo::Pg::PubSub;
use Mojo::URL;
use Scalar::Util 'weaken';

has [qw(auto_migrate search_path)];
has dsn             => 'dbi:Pg:';
has max_connections => 5;
has migrations      => sub {
  my $migrations = Mojo::Pg::Migrations->new(pg => shift);
  weaken $migrations->{pg};
  return $migrations;
};
has options => sub {
  {AutoCommit => 1, AutoInactiveDestroy => 1, PrintError => 0, RaiseError => 1};
};
has [qw(password username)] => '';
has pubsub => sub {
  my $pubsub = Mojo::Pg::PubSub->new(pg => shift);
  weaken $pubsub->{pg};
  return $pubsub;
};

our $VERSION = '2.29';

sub db {
  my $self = shift;

  # Fork-safety
  delete @$self{qw(pid queue)} unless ($self->{pid} //= $$) eq $$;

  return Mojo::Pg::Database->new(dbh => $self->_dequeue, pg => $self);
}

sub from_string {
  my ($self, $str) = @_;

  # Protocol
  return $self unless $str;
  my $url = Mojo::URL->new($str);
  croak qq{Invalid PostgreSQL connection string "$str"}
    unless $url->protocol eq 'postgresql';

  # Connection information
  my $db = $url->path->parts->[0];
  my $dsn = defined $db ? "dbi:Pg:dbname=$db" : 'dbi:Pg:';
  if (my $host = $url->host) { $dsn .= ";host=$host" }
  if (my $port = $url->port) { $dsn .= ";port=$port" }
  if (defined(my $username = $url->username)) { $self->username($username) }
  if (defined(my $password = $url->password)) { $self->password($password) }

  # Service
  my $hash = $url->query->to_hash;
  if (my $service = delete $hash->{service}) { $dsn .= "service=$service" }

  # Options
  @{$self->options}{keys %$hash} = values %$hash;

  return $self->dsn($dsn);
}

sub new { @_ > 1 ? shift->SUPER::new->from_string(@_) : shift->SUPER::new }

sub _dequeue {
  my $self = shift;

  while (my $dbh = shift @{$self->{queue} || []}) { return $dbh if $dbh->ping }
  my $dbh = DBI->connect(map { $self->$_ } qw(dsn username password options));
  if (my $path = $self->search_path) {
    my $search_path = join ', ', map { $dbh->quote_identifier($_) } @$path;
    $dbh->do("set search_path to $search_path");
  }
  ++$self->{migrated} and $self->migrations->migrate
    if !$self->{migrated} && $self->auto_migrate;
  $self->emit(connection => $dbh);

  return $dbh;
}

sub _enqueue {
  my ($self, $dbh) = @_;
  my $queue = $self->{queue} ||= [];
  push @$queue, $dbh if $dbh->{Active};
  shift @$queue while @$queue > $self->max_connections;
}

1;

=encoding utf8

=head1 NAME

Mojo::Pg - Mojolicious ♥ PostgreSQL

=head1 SYNOPSIS

  use Mojo::Pg;

  # Select the server version
  my $pg = Mojo::Pg->new('postgresql://postgres@/test');
  say $pg->db->query('select version() as version')->hash->{version};

  # Use migrations to create a table
  $pg->migrations->name('my_names_app')->from_string(<<EOF)->migrate;
  -- 1 up
  create table names (id serial primary key, name text);
  -- 1 down
  drop table names;
  EOF

  # Use migrations to drop and recreate the table
  $pg->migrations->migrate(0)->migrate;

  # Insert a few rows
  my $db = $pg->db;
  $db->query('insert into names (name) values (?)', 'Sara');
  $db->query('insert into names (name) values (?)', 'Stefan');

  # Insert more rows in a transaction
  eval {
    my $tx = $db->begin;
    $db->query('insert into names (name) values (?)', 'Baerbel');
    $db->query('insert into names (name) values (?)', 'Wolfgang');
    $tx->commit;
  };
  say $@ if $@;

  # Insert another row and return the generated id
  say $db->query('insert into names (name) values (?) returning id', 'Daniel')
    ->hash->{id};

  # JSON roundtrip
  say $db->query('select ?::json as foo', {json => {bar => 'baz'}})
    ->expand->hash->{foo}{bar};

  # Select one row at a time
  my $results = $db->query('select * from names');
  while (my $next = $results->hash) {
    say $next->{name};
  }

  # Select all rows blocking
  say $_->{name} for $db->query('select * from names')->hashes->each;

  # Select all rows non-blocking
  $db->query('select * from names' => sub {
    my ($db, $err, $results) = @_;
    die $err if $err;
    say $_->{name} for $results->hashes->each;
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

  # Concurrent non-blocking queries (synchronized with a delay)
  Mojo::IOLoop->delay(
    sub {
      my $delay = shift;
      $pg->db->query('select now() as now' => $delay->begin);
      $pg->db->query('select * from names' => $delay->begin);
    },
    sub {
      my ($delay, $time_err, $time, $names_err, $names) = @_;
      if (my $err = $time_err || $names_err) { die $err }
      say $time->hash->{now};
      say $_->{name} for $names->hashes->each;
    }
  )->wait;

  # Send and receive notifications non-blocking
  $pg->pubsub->listen(foo => sub {
    my ($pubsub, $payload) = @_;
    say "foo: $payload";
    $pubsub->notify(bar => $payload);
  });
  $pg->pubsub->listen(bar => sub {
    my ($pubsub, $payload) = @_;
    say "bar: $payload";
  });
  $pg->pubsub->notify(foo => 'PostgreSQL rocks!');
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head1 DESCRIPTION

L<Mojo::Pg> is a tiny wrapper around L<DBD::Pg> that makes
L<PostgreSQL|http://www.postgresql.org> a lot of fun to use with the
L<Mojolicious|http://mojolicious.org> real-time web framework.

Database and statement handles are cached automatically, and will be reused
transparently to increase performance. You can handle connection timeouts
gracefully by holding on to them only for short amounts of time.

  use Mojolicious::Lite;
  use Mojo::Pg;

  helper pg => sub { state $pg = Mojo::Pg->new('postgresql://postgres@/test') };

  get '/' => sub {
    my $c  = shift;
    my $db = $c->pg->db;
    $c->render(json => $db->query('select now() as now')->hash);
  };

  app->start;

In this example application, we create a C<pg> helper to store a L<Mojo::Pg>
object. Our action calls that helper and uses the method L<Mojo::Pg/"db"> to
dequeue a L<Mojo::Pg::Database> object from the connection pool. Then we use the
method L<Mojo::Pg::Database/"query"> to execute an
L<SQL|http://www.postgresql.org/docs/current/static/sql.html> statement, which
returns a L<Mojo::Pg::Results> object. And finally we call the method
L<Mojo::Pg::Results/"hash"> to retrieve the first row as a hash reference.

While all I/O operations are performed blocking, you can wait for long running
queries asynchronously, allowing the L<Mojo::IOLoop> event loop to perform
other tasks in the meantime. Since database connections usually have a very low
latency, this often results in very good performance.

Every database connection can only handle one active query at a time, this
includes asynchronous ones. To perform multiple queries concurrently, you have
to use multiple connections.

  # Performed concurrently (5 seconds)
  $pg->db->query('select pg_sleep(5)' => sub {...});
  $pg->db->query('select pg_sleep(5)' => sub {...});

All cached database handles will be reset automatically if a new process has
been forked, this allows multiple processes to share the same L<Mojo::Pg>
object safely.

=head1 GROWING

And as your application grows, you can move queries into model classes.

  package MyApp::Model::Time;
  use Mojo::Base -base;

  has 'pg';

  sub now { shift->pg->db->query('select now() as now')->hash }

  1;

Which get integrated into your application with helpers.

  use Mojolicious::Lite;
  use Mojo::Pg;
  use MyApp::Model::Time;

  helper pg => sub { state $pg = Mojo::Pg->new('postgresql://postgres@/test') };
  helper time => sub { state $time = MyApp::Model::Time->new(pg => shift->pg) };

  get '/' => sub {
    my $c = shift;
    $c->render(json => $c->time->now);
  };

  app->start;

=head1 EVENTS

L<Mojo::Pg> inherits all events from L<Mojo::EventEmitter> and can emit the
following new ones.

=head2 connection

  $pg->on(connection => sub {
    my ($pg, $dbh) = @_;
    ...
  });

Emitted when a new database connection has been established.

  $pg->on(connection => sub {
    my ($pg, $dbh) = @_;
    $dbh->do('set search_path to my_schema');
  });

=head1 ATTRIBUTES

L<Mojo::Pg> implements the following attributes.

=head2 auto_migrate

  my $bool = $pg->auto_migrate;
  $pg      = $pg->auto_migrate($bool);

Automatically migrate to the latest database schema with L</"migrations">, as
soon as the first database connection has been established.

=head2 dsn

  my $dsn = $pg->dsn;
  $pg     = $pg->dsn('dbi:Pg:dbname=foo');

Data source name, defaults to C<dbi:Pg:>.

=head2 max_connections

  my $max = $pg->max_connections;
  $pg     = $pg->max_connections(3);

Maximum number of idle database handles to cache for future use, defaults to
C<5>.

=head2 migrations

  my $migrations = $pg->migrations;
  $pg            = $pg->migrations(Mojo::Pg::Migrations->new);

L<Mojo::Pg::Migrations> object you can use to change your database schema more
easily.

  # Load migrations from file and migrate to latest version
  $pg->migrations->from_file('/home/sri/migrations.sql')->migrate;

=head2 options

  my $options = $pg->options;
  $pg         = $pg->options({AutoCommit => 1, RaiseError => 1});

Options for database handles, defaults to activating C<AutoCommit>,
C<AutoInactiveDestroy> as well as C<RaiseError> and deactivating C<PrintError>.
Note that C<AutoCommit> and C<RaiseError> are considered mandatory, so
deactivating them would be very dangerous.

=head2 password

  my $password = $pg->password;
  $pg          = $pg->password('s3cret');

Database password, defaults to an empty string.

=head2 pubsub

  my $pubsub = $pg->pubsub;
  $pg        = $pg->pubsub(Mojo::Pg::PubSub->new);

L<Mojo::Pg::PubSub> object you can use to send and receive notifications very
efficiently, by sharing a single database connection with many consumers.

  # Subscribe to a channel
  $pg->pubsub->listen(news => sub {
    my ($pubsub, $payload) = @_;
    say "Received: $payload";
  });

  # Notify a channel
  $pg->pubsub->notify(news => 'PostgreSQL rocks!');

=head2 search_path

  my $path = $pg->search_path;
  $pg      = $pg->search_path(['$user', 'foo', 'public']);

Schema search path assigned to all new connections.

  # Isolate tests and avoid race conditions when running them in parallel
  my $pg = Mojo::Pg->new('postgresql:///test')->search_path(['test_one']);
  $pg->db->query('drop schema if exists test_one cascade');
  $pg->db->query('create schema test_one');
  ...
  $pg->db->query('drop schema test_one cascade');

=head2 username

  my $username = $pg->username;
  $pg          = $pg->username('sri');

Database username, defaults to an empty string.

=head1 METHODS

L<Mojo::Pg> inherits all methods from L<Mojo::EventEmitter> and implements the
following new ones.

=head2 db

  my $db = $pg->db;

Get L<Mojo::Pg::Database> object for a cached or newly established database
connection. The L<DBD::Pg> database handle will be automatically cached again
when that object is destroyed, so you can handle problems like connection
timeouts gracefully by holding on to it only for short amounts of time.

  # Add up all the money
  say $pg->db->query('select * from accounts')
    ->hashes->reduce(sub { $a->{money} + $b->{money} });

=head2 from_string

  $pg = $pg->from_string('postgresql://postgres@/test');

Parse configuration from connection string.

  # Just a database
  $pg->from_string('postgresql:///db1');

  # Just a service
  $pg->from_string('postgresql://?service=foo');

  # Username and database
  $pg->from_string('postgresql://sri@/db2');

  # Username, password, host and database
  $pg->from_string('postgresql://sri:s3cret@localhost/db3');

  # Username, domain socket and database
  $pg->from_string('postgresql://sri@%2ftmp%2fpg.sock/db4');

  # Username, database and additional options
  $pg->from_string('postgresql://sri@/db5?PrintError=1&pg_server_prepare=0');

  # Service and additional options
  $pg->from_string('postgresql://?service=foo&PrintError=1&RaiseError=0');

=head2 new

  my $pg = Mojo::Pg->new;
  my $pg = Mojo::Pg->new('postgresql://postgres@/test');

Construct a new L<Mojo::Pg> object and parse connection string with
L</"from_string"> if necessary.

  # Customize configuration further
  my $pg = Mojo::Pg->new->dsn('dbi:Pg:service=foo');

=head1 REFERENCE

This is the class hierarchy of the L<Mojo::Pg> distribution.

=over 2

=item * L<Mojo::Pg>

=item * L<Mojo::Pg::Database>

=item * L<Mojo::Pg::Migrations>

=item * L<Mojo::Pg::PubSub>

=item * L<Mojo::Pg::Results>

=item * L<Mojo::Pg::Transaction>

=back

=head1 AUTHOR

Sebastian Riedel, C<sri@cpan.org>.

=head1 CREDITS

In alphabetical order:

=over 2

Dan Book

Hernan Lopes

William Lindley

=back

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2014-2016, Sebastian Riedel and others.

This program is free software, you can redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<https://github.com/kraih/mojo-pg>, L<Mojolicious::Guides>,
L<http://mojolicious.org>.

=cut
