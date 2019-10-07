package Mojo::Pg::Database;
use Mojo::Base 'Mojo::EventEmitter';

use Carp qw(croak shortmess);
use DBD::Pg ':async';
use Mojo::IOLoop;
use Mojo::JSON 'to_json';
use Mojo::Pg::Results;
use Mojo::Pg::Transaction;
use Mojo::Promise;
use Mojo::Util 'monkey_patch';

has 'dbh';
has pg            => undef, weak => 1;
has results_class => 'Mojo::Pg::Results';

for my $name (qw(delete insert select update)) {
  monkey_patch __PACKAGE__, $name, sub {
    my ($self, @cb) = (shift, ref $_[-1] eq 'CODE' ? pop : ());
    return $self->query($self->pg->abstract->$name(@_), @cb);
  };
  monkey_patch __PACKAGE__, "${name}_p", sub {
    my $self = shift;
    return $self->query_p($self->pg->abstract->$name(@_));
  };
}

sub DESTROY {
  my $self = shift;

  my $waiting = $self->{waiting};
  $waiting->{cb}($self, 'Premature connection close', undef) if $waiting->{cb};

  return unless (my $pg = $self->pg) && (my $dbh = $self->dbh);
  $pg->_enqueue($dbh) unless $dbh->{private_mojo_no_reuse};
}

sub begin { Mojo::Pg::Transaction->new(db => shift) }

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

  for (my $i = 0; $#_ >= $i; $i++) {
    my ($param, $attrs) = ($_[$i], {});
    if (ref $param eq 'HASH') {
      if (exists $param->{json}) { $param = to_json $param->{json} }
      elsif (exists $param->{type} && exists $param->{value}) {
        ($attrs->{pg_type}, $param) = @{$param}{qw(type value)};
      }
    }
    $sth->bind_param($i + 1, $param, $attrs);
  }
  $sth->execute;

  # Blocking
  unless ($cb) {
    $self->_notifications;
    return $self->results_class->new(db => $self, sth => $sth);
  }

  # Non-blocking
  $self->{waiting} = {cb => $cb, sth => $sth};
  $self->_watch;
}

sub query_p {
  my $self    = shift;
  my $promise = Mojo::Promise->new;
  $self->query(
    @_ => sub { $_[1] ? $promise->reject($_[1]) : $promise->resolve($_[2]) });
  return $promise;
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

sub _notifications {
  my $self = shift;

  my $dbh = $self->dbh;
  my $n;
  return undef unless $n = $dbh->pg_notifies;
  while ($n) {
    $self->emit(notification => @$n);
    $n = $dbh->pg_notifies;
  }

  return 1;
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

      return $self->_unwatch if !$self->_notifications && !$self->{waiting};

      return if !$self->{waiting} || !$dbh->pg_ready;
      my ($sth, $cb) = @{delete $self->{waiting}}{qw(sth cb)};

      # Do not raise exceptions inside the event loop
      my $result = do { local $dbh->{RaiseError} = 0; $dbh->pg_result };
      my $err = defined $result ? undef : $dbh->errstr;

      $self->$cb($err, $self->results_class->new(db => $self, sth => $sth));
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

L<Mojo::Pg> object this database belongs to. Note that this attribute is
weakened.

=head2 results_class

  my $class = $db->results_class;
  $db       = $db->results_class('MyApp::Results');

Class to be used by L</"query">, defaults to L<Mojo::Pg::Results>. Note that
this class needs to have already been loaded before L</"query"> is called.

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
    $db->insert('frameworks', {name => 'Catalyst'});
    $db->insert('frameworks', {name => 'Mojolicious'});
    $tx->commit;
  };
  say $@ if $@;

=head2 delete

  my $results = $db->delete($table, \%where, \%options);

Generate a C<DELETE> statement with L<Mojo::Pg/"abstract"> (usually an
L<SQL::Abstract::Pg> object) and execute it with L</"query">. You can also
append a callback to perform operations non-blocking.

  $db->delete(some_table => sub {
    my ($db, $err, $results) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Use all the same argument variations you would pass to the C<delete> method of
L<SQL::Abstract>.

  # "delete from some_table"
  $db->delete('some_table');

  # "delete from some_table where foo = 'bar'"
  $db->delete('some_table', {foo => 'bar'});

  # "delete from some_table where foo like '%test%'"
  $db->delete('some_table', {foo => {-like => '%test%'}});

  # "delete from some_table where foo = 'bar' returning id"
  $db->delete('some_table', {foo => 'bar'}, {returning => 'id'});

=head2 delete_p

  my $promise = $db->delete_p($table, \%where, \%options);

Same as L</"delete">, but performs all operations non-blocking and returns a
L<Mojo::Promise> object instead of accepting a callback.

  $db->delete_p('some_table')->then(sub {
    my $results = shift;
    ...
  })->catch(sub {
    my $err = shift;
    ...
  })->wait;

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

=head2 insert

  my $results = $db->insert($table, \@values || \%fieldvals, \%options);

Generate an C<INSERT> statement with L<Mojo::Pg/"abstract"> (usually an
L<SQL::Abstract::Pg> object) and execute it with L</"query">. You can also
append a callback to perform operations non-blocking.

  $db->insert(some_table => {foo => 'bar'} => sub {
    my ($db, $err, $results) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Use all the same argument variations you would pass to the C<insert> method of
L<SQL::Abstract>.

  # "insert into some_table (foo, baz) values ('bar', 'yada')"
  $db->insert('some_table', {foo => 'bar', baz => 'yada'});

  # "insert into some_table (foo) values ({1,2,3})"
  $db->insert('some_table', {foo => [1, 2, 3]});

  # "insert into some_table (foo) values ('bar') returning id"
  $db->insert('some_table', {foo => 'bar'}, {returning => 'id'});

  # "insert into some_table (foo) values ('bar') returning id, foo"
  $db->insert('some_table', {foo => 'bar'}, {returning => ['id', 'foo']});

As well as some PostgreSQL specific extensions added by L<SQL::Abstract::Pg>.

  # "insert into some_table (foo) values ('bar') on conflict do nothing"
  $db->insert('some_table', {foo => 'bar'}, {on_conflict => undef});

Including operations commonly referred to as C<upsert>.

  # "insert into t (a) values ('b') on conflict (a) do update set a = 'c'"
  $db->insert('t', {a => 'b'}, {on_conflict => [a => {a => 'c'}]});

  # "insert into t (a, b) values ('c', 'd')
  #  on conflict (a, b) do update set a = 'e'"
  $db->insert(
    't', {a => 'c', b => 'd'}, {on_conflict => [['a', 'b'] => {a => 'e'}]});

=head2 insert_p

  my $promise = $db->insert_p($table, \@values || \%fieldvals, \%options);

Same as L</"insert">, but performs all operations non-blocking and returns a
L<Mojo::Promise> object instead of accepting a callback.

  $db->insert_p(some_table => {foo => 'bar'})->then(sub {
    my $results = shift;
    ...
  })->catch(sub {
    my $err = shift;
    ...
  })->wait;

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
statement and return a results object based on L</"results_class"> (which is
usually L<Mojo::Pg::Results>) with the query results. The L<DBD::Pg> statement
handle will be automatically reused when it is not active anymore, to increase
the performance of future queries. You can also append a callback to perform
operations non-blocking.

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

Hash reference arguments containing values named C<type> and C<value>, can be
used to bind specific L<DBD::Pg> data types to placeholders.

  # Insert binary data
  use DBD::Pg ':pg_types';
  $db->query('insert into bar values (?)', {type => PG_BYTEA, value => $bytes});

=head2 query_p

  my $promise = $db->query_p('select * from foo');

Same as L</"query">, but performs all operations non-blocking and returns a
L<Mojo::Promise> object instead of accepting a callback.

  $db->query_p('insert into foo values (?, ?, ?)' => @values)->then(sub {
    my $results = shift;
    ...
  })->catch(sub {
    my $err = shift;
    ...
  })->wait;

=head2 select

  my $results = $db->select($source, $fields, $where, \%options);

Generate a C<SELECT> statement with L<Mojo::Pg/"abstract"> (usually an
L<SQL::Abstract::Pg> object) and execute it with L</"query">. You can also
append a callback to perform operations non-blocking.

  $db->select(some_table => ['foo'] => {bar => 'yada'} => sub {
    my ($db, $err, $results) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Use all the same argument variations you would pass to the C<select> method of
L<SQL::Abstract>.

  # "select * from some_table"
  $db->select('some_table');

  # "select id, foo from some_table"
  $db->select('some_table', ['id', 'foo']);

  # "select * from some_table where foo = 'bar'"
  $db->select('some_table', undef, {foo => 'bar'});

  # "select * from some_table where foo like '%test%'"
  $db->select('some_table', undef, {foo => {-like => '%test%'}});

As well as some PostgreSQL specific extensions added by L<SQL::Abstract::Pg>.

  # "select * from foo join bar on (bar.foo_id = foo.id)"
  $db->select(['foo', ['bar', foo_id => 'id']]);

  # "select * from foo left join bar on (bar.foo_id = foo.id)"
  $db->select(['foo', [-left => 'bar', foo_id => 'id']]);

  # "select foo as bar from some_table"
  $db->select('some_table', [[foo => 'bar']]);

  # "select * from some_table where foo = '[1,2,3]'"
  $db->select('some_table', '*', {foo => {'=' => {-json => [1, 2, 3]}}});

  # "select extract(epoch from foo) as foo, bar from some_table"
  $db->select('some_table', [\'extract(epoch from foo) as foo', 'bar']);

  # "select 'test' as foo, bar from some_table"
  $db->select('some_table', [\['? as foo', 'test'], 'bar']);

Including a new last argument to pass many new options.

  # "select * from some_table where foo = 'bar' order by id desc"
  $db->select('some_table', '*', {foo => 'bar'}, {order_by => {-desc => 'id'}});

  # "select * from some_table limit 10 offset 20"
  $db->select('some_table', '*', undef, {limit => 10, offset => 20});

  # "select * from some_table where foo = 23 group by foo, bar"
  $db->select('some_table', '*', {foo => 23}, {group_by => ['foo', 'bar']});

  # "select * from t where a = 'b' group by c having d = 'e'"
  $db->select('t', '*', {a => 'b'}, {group_by => ['c'], having => {d => 'e'}});

  # "select * from some_table where id = 1 for update"
  $db->select('some_table', '*', {id => 1}, {for => 'update'});

  # "select * from some_table where id = 1 for update skip locked"
  $db->select('some_table', '*', {id => 1}, {for => \'update skip locked'});

=head2 select_p

  my $promise = $db->select_p($source, $fields, $where, \%options);

Same as L</"select">, but performs all operations non-blocking and returns a
L<Mojo::Promise> object instead of accepting a callback.

  $db->select_p(some_table => ['foo'] => {bar => 'yada'})->then(sub {
    my $results = shift;
    ...
  })->catch(sub {
    my $err = shift;
    ...
  })->wait;

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

=head2 update

  my $results = $db->update($table, \%fieldvals, \%where, \%options);

Generate an C<UPDATE> statement with L<Mojo::Pg/"abstract"> (usually an
L<SQL::Abstract::Pg> object) and execute it with L</"query">. You can also
append a callback to perform operations non-blocking.

  $db->update(some_table => {foo => 'baz'} => {foo => 'bar'} => sub {
    my ($db, $err, $results) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Use all the same argument variations you would pass to the C<update> method of
L<SQL::Abstract>.

  # "update some_table set foo = 'bar' where id = 23"
  $db->update('some_table', {foo => 'bar'}, {id => 23});

  # "update some_table set foo = {1,2,3} where id = 23"
  $db->update('some_table', {foo => [1, 2, 3]}, {id => 23});

  # "update some_table set foo = 'bar' where foo like '%test%'"
  $db->update('some_table', {foo => 'bar'}, {foo => {-like => '%test%'}});

  # "update some_table set foo = 'bar' where id = 23 returning id"
  $db->update('some_table', {foo => 'bar'}, {id => 23}, {returning => 'id'});

  # "update some_table set foo = '[1,2,3]' where bar = 23"
  $db->update('some_table', {foo => {-json => [1, 2, 3]}}, {bar => 23});

=head2 update_p

  my $promise = $db->update_p($table, \%fieldvals, \%where, \%options);

Same as L</"update">, but performs all operations non-blocking and returns a
L<Mojo::Promise> object instead of accepting a
callback.

  $db->update_p(some_table => {foo => 'baz'} => {foo => 'bar'})->then(sub {
    my $results = shift;
    ...
  })->catch(sub {
    my $err = shift;
    ...
  })->wait;

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<https://mojolicious.org>.

=cut
