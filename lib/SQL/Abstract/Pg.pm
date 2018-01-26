package SQL::Abstract::Pg;
use Mojo::Base 'SQL::Abstract';

sub _order_by {
  my ($self, $arg) = @_;

  # Legacy
  return $self->SUPER::_order_by($arg)
    if ref $arg ne 'HASH'
    or grep {/^-(?:desc|asc)/i} keys %$arg;

  return $self->_parse($arg);
}

sub _parse {
  my ($self, $options) = @_;

  # ORDER BY
  my $sql = '';
  my @bind;
  if (exists $options->{order_by}) {
    $sql .= $self->_order_by($options->{order_by});
  }

  # LIMIT
  if (defined $options->{limit}) {
    $sql .= ' LIMIT ?';
    push @bind, $options->{limit};
  }

  # OFFSET
  if (defined $options->{offset}) {
    $sql .= ' OFFSET ?';
    push @bind, $options->{offset};
  }

  return $sql, @bind;
}

1;

=encoding utf8

=head1 NAME

SQL::Abstract::Pg - PostgreSQL Magic

=head1 SYNOPSIS

  my $abstract = SQL::Abstract::Pg->new;

=head1 DESCRIPTION

L<SQL::Abstract::Pg> extends L<SQL::Abstract> with a few PostgreSQL features
used by L<Mojo::Pg>.

=head1 METHODS

L<SQL::Abstract::Pg> inherits all methods from L<SQL::Abstract>.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<http://mojolicious.org>.

=cut
