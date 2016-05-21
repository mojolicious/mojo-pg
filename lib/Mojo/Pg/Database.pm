package Mojo::Pg::Database;
use Mojo::Base 'Mojo::EventEmitter';

use Carp qw(croak shortmess);
use DBD::Pg ':async';
use Mojo::IOLoop;
use Mojo::JSON 'to_json';
use Mojo::Pg::Results;
use Mojo::Pg::Transaction;
use Scalar::Util 'weaken';

has [qw(dbh pg)];

sub DESTROY {
  my $self = shift;

  my $waiting = $self->{waiting};
  $waiting->{cb}($self, 'Premature connection close', undef) if $waiting->{cb};

  return unless (my $pg = $self->pg) && (my $dbh = $self->dbh);
  $pg->_enqueue($dbh);
}

sub begin {
  my $self = shift;
  my $tx = Mojo::Pg::Transaction->new(db => $self);
  weaken $tx->{db};
  return $tx;
}

sub disconnect {
  my $self = shift;
  $self->_unwatch;
  $self->dbh->disconnect;
}

sub dollar_only { ++$_[0]{dollar_only} and return $_[0] }

sub is_listening { !!keys %{shift->{listen} || {}} }

sub listen {
  my ($self, $name) = @_;

  my $dbh = $self->dbh;
  $dbh->do('listen ' . $dbh->quote_identifier($name))
    unless $self->{listen}{$name}++;
  $self->_watch;

  return $self;
}

sub notify {
  my ($self, $name, $payload) = @_;

  my $dbh    = $self->dbh;
  my $notify = 'notify ' . $dbh->quote_identifier($name);
  $notify .= ', ' . $dbh->quote($payload) if defined $payload;
  $dbh->do($notify);
  $self->_notifications;

  return $self;
}

sub pid { shift->dbh->{pg_pid} }

sub ping { shift->dbh->ping }

sub query {
  my ($self, $query) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  croak 'Non-blocking query already in progress' if $self->{waiting};

  my %attrs;
  $attrs{pg_placeholder_dollaronly} = 1        if delete $self->{dollar_only};
  $attrs{pg_async}                  = PG_ASYNC if $cb;
  my $sth = $self->dbh->prepare_cached($query, \%attrs, 3);
  local $sth->{HandleError} = sub { $_[0] = shortmess $_[0]; 0 };
  $sth->execute(map { _json($_) ? to_json $_->{json} : $_ } @_);

  # Blocking
  unless ($cb) {
    $self->_notifications;
    return Mojo::Pg::Results->new(sth => $sth);
  }

  # Non-blocking
  $self->{waiting} = {cb => $cb, sth => $sth};
  $self->_watch;
}

sub tables {
  my @tables = shift->dbh->tables('', '', '', '');
  return [grep { $_ !~ /^(?:pg_catalog|information_schema)\./ } @tables];
}

sub unlisten {
  my ($self, $name) = @_;

  my $dbh = $self->dbh;
  $dbh->do('unlisten ' . $dbh->quote_identifier($name));
  $name eq '*' ? delete $self->{listen} : delete $self->{listen}{$name};
  $self->_unwatch unless $self->{waiting} || $self->is_listening;

  return $self;
}

sub _json { ref $_[0] eq 'HASH' && exists $_[0]{json} }

sub _notifications {
  my $self = shift;
  my $dbh  = $self->dbh;
  while (my $n = $dbh->pg_notifies) { $self->emit(notification => @$n) }
}

sub _unwatch {
  my $self = shift;
  return unless delete $self->{watching};
  Mojo::IOLoop->singleton->reactor->remove($self->{handle});
  $self->emit('close') if $self->is_listening;
}

sub _watch {
  my $self = shift;

  return if $self->{watching} || $self->{watching}++;

  my $dbh = $self->dbh;
  unless ($self->{handle}) {
    open $self->{handle}, '<&', $dbh->{pg_socket} or die "Can't dup: $!";
  }
  Mojo::IOLoop->singleton->reactor->io(
    $self->{handle} => sub {
      my $reactor = shift;

      $self->_unwatch if !eval { $self->_notifications; 1 };
      return unless $self->{waiting} && $dbh->pg_ready;
      my ($sth, $cb) = @{delete $self->{waiting}}{qw(sth cb)};

      # Do not raise exceptions inside the event loop
      my $result = do { local $dbh->{RaiseError} = 0; $dbh->pg_result };
      my $err = defined $result ? undef : $dbh->errstr;

      $self->$cb($err, Mojo::Pg::Results->new(sth => $sth));
      $self->_unwatch unless $self->{waiting} || $self->is_listening;
    }
  )->watch($self->{handle}, 1, 0);
}

1;

=encoding utf8

=head1 NAME

Mojo::Pg::Database - Database

=head1 SYNOPSIS

  use Mojo::Pg::Database;

  my $db = Mojo::Pg::Database->new(pg => $pg, dbh => $dbh);
  $db->query('select * from foo')
    ->hashes->map(sub { $_->{bar} })->join("\n")->say;

=head1 DESCRIPTION

L<Mojo::Pg::Database> is a container for L<DBD::Pg> database handles used by
L<Mojo::Pg>.

=head1 EVENTS

L<Mojo::Pg::Database> inherits all events from L<Mojo::EventEmitter> and can
emit the following new ones.

=head2 close

  $db->on(close => sub {
    my $db = shift;
    ...
  });

Emitted when the database connection gets closed while waiting for
notifications.

=head2 notification

  $db->on(notification => sub {
    my ($db, $name, $pid, $payload) = @_;
    ...
  });

Emitted when a notification has been received.

=head1 ATTRIBUTES

L<Mojo::Pg::Database> implements the following attributes.

=head2 dbh

  my $dbh = $db->dbh;
  $db     = $db->dbh($dbh);

L<DBD::Pg> database handle used for all queries.

  # Use DBI utility methods
  my $quoted = $db->dbh->quote_identifier('foo.bar');

=head2 pg

  my $pg = $db->pg;
  $db    = $db->pg(Mojo::Pg->new);

L<Mojo::Pg> object this database belongs to.

=head1 METHODS

L<Mojo::Pg::Database> inherits all methods from L<Mojo::EventEmitter> and
implements the following new ones.

=head2 begin

  my $tx = $db->begin;

Begin transaction and return L<Mojo::Pg::Transaction> object, which will
automatically roll back the transaction unless
L<Mojo::Pg::Transaction/"commit"> has been called before it is destroyed.

  # Insert rows in a transaction
  eval {
    my $tx = $db->begin;
    $db->query('insert into frameworks values (?)', 'Catalyst');
    $db->query('insert into frameworks values (?)', 'Mojolicious');
    $tx->commit;
  };
  say $@ if $@;

=head2 disconnect

  $db->disconnect;

Disconnect L</"dbh"> and prevent it from getting reused.

=head2 dollar_only

  $db = $db->dollar_only;

Activate C<pg_placeholder_dollaronly> for next L</"query"> call and allow C<?>
to be used as an operator.

  # Check for a key in a JSON document
  $db->dollar_only->query('select * from foo where bar ? $1', 'baz')
    ->expand->hashes->map(sub { $_->{bar}{baz} })->join("\n")->say;

=head2 is_listening

  my $bool = $db->is_listening;

Check if L</"dbh"> is listening for notifications.

=head2 listen

  $db = $db->listen('foo');

Subscribe to a channel and receive L</"notification"> events when the
L<Mojo::IOLoop> event loop is running.

=head2 notify

  $db = $db->notify('foo');
  $db = $db->notify(foo => 'bar');

Notify a channel.

=head2 pid

  my $pid = $db->pid;

Return the process id of the backend server process.

=head2 ping

  my $bool = $db->ping;

Check database connection.

=head2 query

  my $results = $db->query('select * from foo');
  my $results = $db->query('insert into foo values (?, ?, ?)', @values);
  my $results = $db->query('select ?::json as foo', {json => {bar => 'baz'}});

Execute a blocking L<SQL|http://www.postgresql.org/docs/current/static/sql.html>
statement and return a L<Mojo::Pg::Results> object with the results. The
L<DBD::Pg> statement handle will be automatically reused when it is not active
anymore, to increase the performance of future queries. You can also append a
callback to perform operations non-blocking.

  $db->query('insert into foo values (?, ?, ?)' => @values => sub {
    my ($db, $err, $results) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Hash reference arguments containing a value named C<json>, will be encoded to
JSON text with L<Mojo::JSON/"to_json">. To accomplish the reverse, you can use
the method L<Mojo::Pg::Results/"expand">, which automatically decodes all fields
of the types C<json> and C<jsonb> with L<Mojo::JSON/"from_json"> to Perl values.

  # "I ♥ Mojolicious!"
  $db->query('select ?::jsonb as foo', {json => {bar => 'I ♥ Mojolicious!'}})
    ->expand->hash->{foo}{bar};

=head2 tables

  my $tables = $db->tables;

Return table and view names for this database, that are visible to the current
user and not internal, as an array reference.

  # Names of all tables
  say for @{$db->tables};

=head2 unlisten

  $db = $db->unlisten('foo');
  $db = $db->unlisten('*');

Unsubscribe from a channel, C<*> can be used to unsubscribe from all channels.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<http://mojolicious.org>.

=cut
