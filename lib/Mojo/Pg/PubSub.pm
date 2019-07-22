package Mojo::Pg::PubSub;
use Mojo::Base 'Mojo::EventEmitter';

use Mojo::JSON qw(from_json to_json);
use Scalar::Util 'weaken';

has pg                 => undef, weak => 1;
has reconnect_interval => 1;

sub db {
  my $self = shift;

  return $self->{db} if $self->{db};

  my $db = $self->{db} = $self->pg->db;
  weaken $self;
  $db->on(
    notification => sub {
      my ($db, $name, $pid, $payload) = @_;
      $payload = eval { from_json $payload } if $self->{json}{$name};
      my @cbs = @{$self->{chans}{$name}};
      for my $cb (@cbs) { $self->$cb($payload) }
    }
  );

  $db->once(close => sub { $self->emit(disconnect => delete $self->{db}) });
  $db->listen($_) for keys %{$self->{chans}}, 'mojo.pubsub';
  delete $self->{reconnecting};
  $self->emit(reconnect => $db);

  return $db;
}

sub DESTROY { Mojo::Util::_global_destruction() or shift->reset }

sub json { ++$_[0]{json}{$_[1]} and return $_[0] }

sub listen {
  my ($self, $name, $cb) = @_;
  $self->db->listen($name)
    if !@{$self->{chans}{$name} ||= []} && !$self->{reconnecting};
  push @{$self->{chans}{$name}}, $cb;
  return $cb;
}

sub new {
  my $self = shift->SUPER::new(@_);
  $self->on(disconnect => \&_disconnect);
  return $self;
}

sub notify { $_[0]->db->notify(_json(@_)) and return $_[0] }

sub reset {
  my $self = shift;
  delete @$self{qw(chans json pid)};
  return unless my $db = delete $self->{db};
  ++$db->dbh->{private_mojo_no_reuse} and $db->_unwatch;
}

sub unlisten {
  my ($self, $name, $cb) = @_;

  my $chan = $self->{chans}{$name};
  unless (@$chan = $cb ? grep { $cb ne $_ } @$chan : ()) {
    $self->db->unlisten($name) unless $self->{reconnecting};
    delete $self->{chans}{$name};
  }

  return $self;
}

sub _disconnect {
  my $self = shift;

  $self->{reconnecting} = 1;

  weaken $self;
  my $r;
  $r = Mojo::IOLoop->recurring(
    $self->reconnect_interval => sub {
      Mojo::IOLoop->remove($r) if eval { $self->db };
    }
  );
}

sub _json { $_[1], $_[0]{json}{$_[1]} ? to_json $_[2] : $_[2] }

1;

=encoding utf8

=head1 NAME

Mojo::Pg::PubSub - Publish/Subscribe

=head1 SYNOPSIS

  use Mojo::Pg::PubSub;

  my $pubsub = Mojo::Pg::PubSub->new(pg => $pg);
  my $cb = $pubsub->listen(foo => sub {
    my ($pubsub, $payload) = @_;
    say "Received: $payload";
  });
  $pubsub->notify(foo => 'I ♥ Mojolicious!');
  $pubsub->unlisten(foo => $cb);

=head1 DESCRIPTION

L<Mojo::Pg::PubSub> is a scalable implementation of the publish/subscribe
pattern used by L<Mojo::Pg>. It is based on PostgreSQL notifications and allows
many consumers to share the same database connection, to avoid many common
scalability problems.

=head1 EVENTS

L<Mojo::Pg::PubSub> inherits all events from L<Mojo::EventEmitter> and can
emit the following new ones.

=head2 disconnect

  $pubsub->on(disconnect => sub {
    my ($pubsub, $db) = @_;
    ...
  });

Emitted after the current database connection is lost.

=head2 reconnect

  $pubsub->on(reconnect => sub {
    my ($pubsub, $db) = @_;
    ...
  });

Emitted after switching to a new database connection for sending and receiving
notifications.

=head1 ATTRIBUTES

L<Mojo::Pg::PubSub> implements the following attributes.

=head2 pg

  my $pg  = $pubsub->pg;
  $pubsub = $pubsub->pg(Mojo::Pg->new);

L<Mojo::Pg> object this publish/subscribe container belongs to. Note that this
attribute is weakened.

=head2 reconnect_interval

  my $interval = $pubsub->reconnect_interval;
  $pubsub      = $pubsub->reconnect_interval(0.1);

Amount of time in seconds to wait to reconnect after disconnecting, defaults to
C<1>.

=head1 METHODS

L<Mojo::Pg::PubSub> inherits all methods from L<Mojo::EventEmitter> and
implements the following new ones.

=head2 db

  my $db = $pubsub->db;

Build and cache or get cached L<Mojo::Pg::Database> connection from L</"pg">.
Used to reconnect if disconnected.

  # Reconnect immediately
  $pubsub->unsubscribe('disconnect')->on(disconnect => sub { shift->db });

=head2 json

  $pubsub = $pubsub->json('foo');

Activate automatic JSON encoding and decoding with L<Mojo::JSON/"to_json"> and
L<Mojo::JSON/"from_json"> for a channel.

  # Send and receive data structures
  $pubsub->json('foo')->listen(foo => sub {
    my ($pubsub, $payload) = @_;
    say $payload->{bar};
  });
  $pubsub->notify(foo => {bar => 'I ♥ Mojolicious!'});

=head2 listen

  my $cb = $pubsub->listen(foo => sub {...});

Subscribe to a channel, there is no limit on how many subscribers a channel can
have. Automatic decoding of JSON text to Perl values can be activated with
L</"json">.

  # Subscribe to the same channel twice
  $pubsub->listen(foo => sub {
    my ($pubsub, $payload) = @_;
    say "One: $payload";
  });
  $pubsub->listen(foo => sub {
    my ($pubsub, $payload) = @_;
    say "Two: $payload";
  });

=head2 new

  my $pubsub = Mojo::Pg::PubSub->new;
  my $pubsub = Mojo::Pg::PubSub->new(pg => Mojo::Pg->new);
  my $pubsub = Mojo::Pg::PubSub->new({pg => Mojo::Pg->new});

Construct a new L<Mojo::Pg::PubSub> object and subscribe to the L</"disconnect">
event with default reconnect logic.

=head2 notify

  $pubsub = $pubsub->notify('foo');
  $pubsub = $pubsub->notify(foo => 'I ♥ Mojolicious!');
  $pubsub = $pubsub->notify(foo => {bar => 'baz'});

Notify a channel. Automatic encoding of Perl values to JSON text can be
activated with L</"json">.

=head2 reset

  $pubsub->reset;

Reset all subscriptions and the database connection. This is usually done after
a new process has been forked, to prevent the child process from stealing
notifications meant for the parent process.

=head2 unlisten

  $pubsub = $pubsub->unlisten('foo');
  $pubsub = $pubsub->unlisten(foo => $cb);

Unsubscribe from a channel.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<https://mojolicious.org>.

=cut
