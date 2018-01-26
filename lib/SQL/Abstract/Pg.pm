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

=head2 ORDER BY

  $abstract->select($source, $fields, $where, $order);
  $abstract->select($source, $fields, $where, \%options);

Alternatively to the C<$order> argument accepted by L<SQL::Abstract> you can now
also pass a hash reference with various options. This includes C<order_by>,
which takes the same values as the C<$order> argument.

  # "select * from some_table order by foo desc"
  $abstract->select('some_table', undef, undef, {order_by => {-desc => 'foo'}});

=head2 LIMIT/OFFSET

The C<limit> and C<offset> options can be used to generate C<SELECT> queries
with C<LIMIT> and C<OFFSET> clauses.

  # "select * from some_table limit 10"
  $abstract->select('some_table', undef, undef, {limit => 10});

  # "select * from some_table offset 5"
  $abstract->select('some_table', undef, undef, {offset => 5});

  # "select * from some_table limit 10 offset 5"
  $abstract->select('some_table', undef, undef, {limit => 10, offset => 5});

=head1 METHODS

L<SQL::Abstract::Pg> inherits all methods from L<SQL::Abstract>.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<http://mojolicious.org>.

=cut
