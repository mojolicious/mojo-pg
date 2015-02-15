package Mojo::Pg::PubSub;
use Mojo::Base 'Mojo::EventEmitter';

use Scalar::Util 'weaken';

has db => sub {
  my $self = shift;

  my $db = $self->{db} = $self->pg->db;
  weaken $db->{pg};
  weaken $self;
  $db->on(
    notification => sub {
      my ($db, $name, $pid, $payload) = @_;
      for my $cb (@{$self->{chans}{$name}}) { $self->$cb($payload) }
    }
  );
  $db->once(
    close => sub {
      delete $self->{db};
      eval { $self->db };
    }
  );
  $db->listen($_) for keys %{$self->{chans}};
  $self->emit(reconnect => $db);

  return $db;
};
has 'pg';

sub listen {
  my ($self, $name, $cb) = @_;
  $self->db->listen($name) unless @{$self->{chans}{$name} ||= []};
  push @{$self->{chans}{$name}}, $cb;
  return $cb;
}

sub notify { $_[0]->db->notify(@_[1, 2]) and return $_[0] }

sub unlisten {
  my ($self, $name, $cb) = @_;
  my $chan = $self->{chans}{$name};
  @$chan = grep { $cb ne $_ } @$chan;
  $self->db->unlisten($name) and delete $self->{chans}{$name} unless @$chan;
  return $self;
}

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
  $pubsub->notify(foo => 'bar');
  $pubsub->unlisten(foo => $cb);

=head1 DESCRIPTION

L<Mojo::Pg::PubSub> is a scalable implementation of the publish/subscribe
pattern used by L<Mojo::Pg>. It is based on PostgreSQL notifications and
allows many consumers to share the same database connection, to avoid many
common scalability problems.

Note that this module is EXPERIMENTAL and might change without warning!

=head1 EVENTS

L<Mojo::Pg::PubSub> inherits all events from L<Mojo::EventEmitter> and can
emit the following new ones.

=head2 reconnect

  $pubsub->on(reconnect => sub {
    my ($pubsub, $db) = @_;
    ...
  });

Emitted when a new database connection has been established.

=head1 ATTRIBUTES

L<Mojo::Pg::PubSub> implements the following attributes.

=head2 db

  my $db  = $pubsub->db;
  $pubsub = $pubsub->db(Mojo::Pg::Database->new);

L<Mojo::Pg::Database> object that is currently being used to send and receive
notifications.

=head2 pg

  my $pg  = $pubsub->pg;
  $pubsub = $pubsub->pg(Mojo::Pg->new);

L<Mojo::Pg> object this publish/subscribe container belongs to.

=head1 METHODS

L<Mojo::Pg::PubSub> inherits all methods from L<Mojo::EventEmitter> and
implements the following new ones.

=head2 listen

  my $cb = $pubsub->listen(foo => sub {...});

Subscribe to a channel, there is no limit on how many subscribers a channel
can have.

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

  $pubsub = $pubsub->notify(foo => 'bar');

Notify a channel.

=head2 unlisten

  $pubsub = $pubsub->unlisten(foo => $cb);

Unsubscribe from a channel.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
