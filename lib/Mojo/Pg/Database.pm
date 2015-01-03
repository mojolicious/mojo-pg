package Mojo::Pg::Database;
use Mojo::Base 'Mojo::EventEmitter';

use DBD::Pg ':async';
use IO::Handle;
use Mojo::IOLoop;
use Mojo::JSON 'encode_json';
use Mojo::Pg::Results;
use Mojo::Pg::Transaction;
use Scalar::Util 'weaken';

has [qw(dbh pg)];

sub DESTROY {
  my $self = shift;
  return unless my $pg = $self->pg;
  if (my $dbh = $self->dbh) { $pg->_enqueue($dbh, $self->{handle}) }
}

sub backlog { scalar @{shift->{waiting} || []} }

sub begin {
  my $self = shift;
  $self->dbh->begin_work;
  my $tx = Mojo::Pg::Transaction->new(db => $self);
  weaken $tx->{db};
  return $tx;
}

sub disconnect {
  my $self = shift;
  $self->_unwatch;
  delete $self->{queue};
  $self->dbh->disconnect;
}

sub do {
  my $self = shift;
  $self->dbh->do(@_);
  $self->_notifications;
  return $self;
}

sub dollar_only { ++$_[0]{dollar_only} and return $_[0] }

sub is_listening { !!keys %{shift->{listen} || {}} }

sub listen {
  my ($self, $name) = @_;

  my $dbh = $self->dbh;
  local $dbh->{AutoCommit} = 1;
  $dbh->do('listen ' . $dbh->quote_identifier($name))
    unless $self->{listen}{$name}++;
  $self->_watch;

  return $self;
}

sub notify {
  my ($self, $name, $payload) = @_;
  $self->query('select pg_notify($1, $2)', $name, $payload);
  return $self;
}

sub pid { shift->dbh->{pg_pid} }

sub ping { shift->dbh->ping }

sub query {
  my ($self, $query) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  # JSON
  my @values = map { _json($_) ? encode_json $_->{json} : $_ } @_;

  # Dollar only
  my $dbh = $self->dbh;
  local $dbh->{pg_placeholder_dollaronly} = 1 if delete $self->{dollar_only};

  # Blocking
  unless ($cb) {
    my $sth = $dbh->prepare($query);
    $sth->execute(@values);
    $self->_notifications;
    return Mojo::Pg::Results->new(sth => $sth);
  }

  # Non-blocking
  my $sth = $dbh->prepare($query, {pg_async => PG_ASYNC});
  push @{$self->{waiting}}, {args => \@values, cb => $cb, sth => $sth};
  $self->$_ for qw(_next _watch);
}

sub unlisten {
  my ($self, $name) = @_;

  my $dbh = $self->dbh;
  local $dbh->{AutoCommit} = 1;
  $dbh->do('unlisten' . $dbh->quote_identifier($name));
  $name eq '*' ? delete($self->{listen}) : delete($self->{listen}{$name});
  $self->_unwatch unless $self->backlog || $self->is_listening;

  return $self;
}

sub _json { ref $_[0] eq 'HASH' && (keys %{$_[0]})[0] eq 'json' }

sub _next {
  return unless my $next = shift->{waiting}[0];
  $next->{sth}->execute(@{$next->{args}}) unless $next->{executed}++;
}

sub _notifications {
  my $self = shift;
  while (my $notify = $self->dbh->pg_notifies) {
    $self->emit(notification => @$notify);
  }
}

sub _unwatch {
  my $self = shift;
  return unless delete $self->{watching};
  Mojo::IOLoop->singleton->reactor->remove($self->{handle});
}

sub _watch {
  my $self = shift;

  return if $self->{watching} || $self->{watching}++;

  my $dbh = $self->dbh;
  $self->{handle} ||= IO::Handle->new_from_fd($dbh->{pg_socket}, 'r');
  Mojo::IOLoop->singleton->reactor->io(
    $self->{handle} => sub {
      my $reactor = shift;

      $self->emit('close')->_unwatch
        if !eval { $self->_notifications; 1 } && $self->is_listening;
      return unless (my $waiting = $self->{waiting}) && $dbh->pg_ready;
      my ($sth, $cb) = @{shift @$waiting}{qw(sth cb)};

      # Do not raise exceptions inside the event loop
      my $result = do { local $dbh->{RaiseError} = 0; $dbh->pg_result };
      my $err = defined $result ? undef : $dbh->errstr;

      $self->$cb($err, Mojo::Pg::Results->new(sth => $sth));
      $self->_next;
      $self->_unwatch unless $self->backlog || $self->is_listening;
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
  $db     = $db->dbh(DBI->new);

L<DBD::Pg> database handle used for all queries.

=head2 pg

  my $pg = $db->pg;
  $db    = $db->pg(Mojo::Pg->new);

L<Mojo::Pg> object this database belongs to.

=head1 METHODS

L<Mojo::Pg::Database> inherits all methods from L<Mojo::EventEmitter> and
implements the following new ones.

=head2 backlog

  my $num = $db->backlog;

Number of waiting non-blocking queries.

=head2 begin

  my $tx = $db->begin;

Begin transaction and return L<Mojo::Pg::Transaction> object, which will
automatically roll back the transaction unless
L<Mojo::Pg::Transaction/"commit"> has been called before it is destroyed.

  my $tx = $db->begin;
  $db->query('insert into names values (?)', 'Baerbel');
  $db->query('insert into names values (?)', 'Wolfgang');
  $tx->commit;

=head2 disconnect

  $db->disconnect;

Disconnect L</"dbh"> and prevent it from getting cached again.

=head2 do

  $db = $db->do('create table foo (bar text)');

Execute a statement and discard its result.

=head2 dollar_only

  $db = $db->dollar_only;

Activate C<pg_placeholder_dollaronly> for next L</"query"> call and allow C<?>
to be used as an operator.

  $db->dollar_only->query('select * from foo where bar ? $1', 'baz')
    ->expand->hashes->map(sub { $_->{bar}{baz} })->join("\n")->say;

=head2 is_listening

  my $bool = $db->is_listening;

Check if L</"dbh"> is listening for notifications.

=head2 listen

  $db = $db->listen('foo');

Start listening for notifications when the L<Mojo::IOLoop> event loop is
running.

=head2 notify

  $db = $db->notify('foo');
  $db = $db->notify('foo', 'bar');

Send notification with optional payload.

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

Execute a blocking statement and return a L<Mojo::Pg::Results> object with the
results. You can also append a callback to perform operation non-blocking.

  $db->query('insert into foo values (?, ?, ?)' => @values => sub {
    my ($db, $err, $results) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 unlisten

  $db = $db->unlisten('foo');
  $db = $db->unlisten('*');

Stop listening for notifications.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
