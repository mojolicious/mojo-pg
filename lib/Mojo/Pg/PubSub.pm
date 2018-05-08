package Mojo::Pg::PubSub;
use Mojo::Base 'Mojo::EventEmitter';

use Mojo::JSON qw(from_json to_json);
use Scalar::Util 'weaken';

has 'pg';

sub DESTROY { Mojo::Util::_global_destruction() or shift->reset }

sub json { ++$_[0]{json}{$_[1]} and return $_[0] }

sub listen {
  my ($self, $name, $cb) = @_;
  $self->_db->listen($name) unless @{$self->{chans}{$name} ||= []};
  push @{$self->{chans}{$name}}, $cb;
  return $cb;
}

sub notify { $_[0]->_db->notify(_json(@_)) and return $_[0] }

sub reset {
  my $self = shift;
  delete @$self{qw(chans json pid)};
  return unless my $db = delete $self->{db};
  ++$db->dbh->{private_mojo_no_reuse} and $db->_unwatch;
}

sub unlisten {
  my ($self, $name, $cb) = @_;
  my $chan = $self->{chans}{$name};
  @$chan = $cb ? grep { $cb ne $_ } @$chan : ();
  $self->_db->unlisten($name) and delete $self->{chans}{$name} unless @$chan;
  return $self;
}

sub _db {
  my $self = shift;

  return $self->{db} if $self->{db};

  my $db = $self->{db} = $self->pg->db;
  weaken $db->{pg};
  weaken $self;
  $db->on(
    notification => sub {
      my ($db, $name, $pid, $payload) = @_;
      $payload = eval { from_json $payload } if $self->{json}{$name};
      my @cbs = @{$self->{chans}{$name}};
      for my $cb (@cbs) { $self->$cb($payload) }
    }
  );
  $db->once(close => sub { $self->{pg} and $self->_db if delete $self->{db} });
  $db->listen($_) for keys %{$self->{chans}}, 'mojo.pubsub';
  $self->emit(reconnect => $db);

  return $db;
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

L<Mojo::Pg> object this publish/subscribe container belongs to.

=head1 METHODS

L<Mojo::Pg::PubSub> inherits all methods from L<Mojo::EventEmitter> and
implements the following new ones.

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
