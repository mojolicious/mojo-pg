package Mojo::Pg::Database;
use Mojo::Base 'Mojo::EventEmitter';

use DBD::Pg ':async';
use IO::Handle;
use Mojo::IOLoop;
use Mojo::Pg::Results;
use Mojo::Pg::Transaction;

has [qw(dbh pg)];
has max_statements => 10;

sub DESTROY {
  my $self = shift;
  if ((my $dbh = $self->dbh) && (my $pg = $self->pg)) { $pg->_enqueue($dbh) }
}

sub backlog { scalar @{shift->{waiting} || []} }

sub begin {
  my $self = shift;
  my $dbh  = $self->dbh;
  $dbh->begin_work;
  return Mojo::Pg::Transaction->new(dbh => $dbh);
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
  return $self;
}

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
  $self->query('select pg_notify(?, ?)', $name, $payload);
  return $self;
}

sub ping { shift->dbh->ping }

sub query {
  my ($self, $query) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  # Blocking
  unless ($cb) {
    my $sth = $self->_dequeue(0, $query);
    $sth->execute(@_);
    return Mojo::Pg::Results->new(db => $self, sth => $sth);
  }

  # Non-blocking
  push @{$self->{waiting}}, {args => [@_], cb => $cb, query => $query};
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

sub _dequeue {
  my ($self, $async, $query) = @_;

  my $queue = $self->{queue} ||= [];
  for (my $i = 0; $i <= $#$queue; $i++) {
    my $sth = $queue->[$i];
    return splice @$queue, $i, 1
      if !(!$sth->{pg_async} ^ !$async) && $sth->{Statement} eq $query;
  }

  return $self->dbh->prepare($query, $async ? {pg_async => PG_ASYNC} : ());
}

sub _enqueue {
  my ($self, $sth) = @_;
  push @{$self->{queue}}, $sth;
  shift @{$self->{queue}} while @{$self->{queue}} > $self->max_statements;
}

sub _next {
  my $self = shift;

  return unless my $next = $self->{waiting}[0];
  return if $next->{sth};

  my $sth = $next->{sth} = $self->_dequeue(1, $next->{query});
  $sth->execute(@{$next->{args}});
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

      # Notifications
      while (my $notify = $dbh->pg_notifies) {
        $self->emit(notification => @$notify);
      }

      return unless (my $waiting = $self->{waiting}) && $dbh->pg_ready;
      my ($sth, $cb) = @{shift @$waiting}{qw(sth cb)};

      # Do not raise exceptions inside the event loop
      my $result = do { local $dbh->{RaiseError} = 0; $dbh->pg_result };
      my $err = defined $result ? undef : $dbh->errstr;

      $self->$cb($err, Mojo::Pg::Results->new(db => $self, sth => $sth));
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
  $db->query('select * from foo')->hashes->pluck('bar')->join("\n")->say;

=head1 DESCRIPTION

L<Mojo::Pg::Database> is a container for L<DBD::Pg> database handles used by
L<Mojo::Pg>.

=head1 EVENTS

L<Mojo::Pg::Database> inherits all events from L<Mojo::EventEmitter> and can
emit the following new ones.

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

=head2 max_statements

  my $max = $db->max_statements;
  $db     = $db->max_statements(5);

Maximum number of statement handles to cache for future queries, defaults to
C<10>.

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
L<Mojo::Pg::Transaction/"commit"> bas been called before it is destroyed.

  my $tx = $db->begin;
  $db->query('insert into names values (?)', 'Baerbel');
  $db->query('insert into names values (?)', 'Wolfgangl');
  $tx->commit;

=head2 disconnect

  $db->disconnect;

Disconnect L</"dbh"> and prevent it from getting cached again.

=head2 do

  $db = $db->do('create table foo (bar varchar(255))');

Execute a statement and discard its result.

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

=head2 ping

  my $bool = $db->ping;

Check database connection.

=head2 query

  my $results = $db->query('select * from foo');
  my $results = $db->query('insert into foo values (?, ?, ?)', @values);

Execute a blocking statement and return a L<Mojo::Pg::Results> object with the
results. The L<DBD::Pg> statement handle will be automatically cached again
when that object is destroyed, so future queries can reuse it to increase
performance. You can also append a callback to perform operation non-blocking.

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
