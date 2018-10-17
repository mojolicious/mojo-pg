package Mojo::Pg::Transaction;
use Mojo::Base -base;

has db => undef, weak => 1;

sub DESTROY {
  my $self = shift;
  if ($self->{rollback} && (my $dbh = $self->{dbh})) { $dbh->rollback }
}

sub commit {
  my $self = shift;
  $self->{dbh}->commit if delete $self->{rollback};
  if (my $db = $self->db) { $db->_notifications }
}

sub new {
  my $self = shift->SUPER::new(@_, rollback => 1);
  my $dbh = $self->{dbh} = $self->db->dbh;
  $dbh->begin_work;
  return $self;
}

1;

=encoding utf8

=head1 NAME

Mojo::Pg::Transaction - Transaction

=head1 SYNOPSIS

  use Mojo::Pg::Transaction;

  my $tx = Mojo::Pg::Transaction->new(db => $db);
  $tx->commit;

=head1 DESCRIPTION

L<Mojo::Pg::Transaction> is a scope guard for L<DBD::Pg> transactions used by
L<Mojo::Pg::Database>.

=head1 ATTRIBUTES

L<Mojo::Pg::Transaction> implements the following attributes.

=head2 db

  my $db = $tx->db;
  $tx    = $tx->db(Mojo::Pg::Database->new);

L<Mojo::Pg::Database> object this transaction belongs to. Note that this
attribute is weakened.

=head1 METHODS

L<Mojo::Pg::Transaction> inherits all methods from L<Mojo::Base> and
implements the following new ones.

=head2 commit

  $tx->commit;

Commit transaction.

=head2 new

  my $tx = Mojo::Pg::Transaction->new;
  my $tx = Mojo::Pg::Transaction->new(db => Mojo::Pg::Database->new);
  my $tx = Mojo::Pg::Transaction->new({db => Mojo::Pg::Database->new});

Construct a new L<Mojo::Pg::Transaction> object.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<https://mojolicious.org>.

=cut
