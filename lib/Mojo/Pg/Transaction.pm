package Mojo::Pg::Transaction;
use Mojo::Base -base;

has 'dbh';

sub DESTROY {
  my $self = shift;
  if ($self->{rollback} && (my $dbh = $self->dbh)) { $dbh->rollback }
}

sub commit { $_[0]->dbh->commit if delete $_[0]->{rollback} }

sub new { shift->SUPER::new(@_, rollback => 1) }

1;

=encoding utf8

=head1 NAME

Mojo::Pg::Transaction - Transaction

=head1 SYNOPSIS

  use Mojo::Pg::Transaction;

  my $tx = Mojo::Pg::Transaction->new(dbh => $dbh);
  $tx->commit;

=head1 DESCRIPTION

L<Mojo::Pg::Transaction> is a scope guard for L<DBD::Pg> transactions used by
L<Mojo::Pg::Database>.

=head1 ATTRIBUTES

L<Mojo::Pg::Transaction> implements the following attributes.

=head2 dbh

  my $dbh = $tx->dbh;
  $tx     = $tx->dbh($dbh);

L<DBD::Pg> database handle this transaction belongs to.

=head1 METHODS

L<Mojo::Pg::Transaction> inherits all methods from L<Mojo::Base> and
implements the following new ones.

=head2 commit

  $tx->commit;

Commit transaction.

=head2 new

  my $tx = Mojo::Pg::Transaction->new;

Construct a new L<Mojo::Pg::Transaction> object.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
