package SQL::Abstract::Pg;
use Mojo::Base 'SQL::Abstract';

sub insert {
  my ($self, $table, $data, $options) = @_;
  local @{$options}{qw(returning _pg_returning)} = (1, 1)
    if defined $options->{on_conflict} && !$options->{returning};
  return $self->SUPER::insert($table, $data, $options);
}

sub _insert_returning {
  my ($self, $options) = @_;

  delete $options->{returning} if $options->{_pg_returning};

  # ON CONFLICT
  my $sql = '';
  my @bind;
  if (defined(my $conflict = $options->{on_conflict})) {
    my ($conflict_sql, @conflict_bind);
    $self->_SWITCH_refkind(
      $conflict => {
        SCALARREF => sub { $conflict_sql = $$conflict },
        ARRAYREFREF => sub { ($conflict_sql, @conflict_bind) = @$$conflict }
      }
    );
    $sql .= $self->_sqlcase(' on conflict ') . $conflict_sql;
    push @bind, @conflict_bind;
  }

  $sql .= $self->SUPER::_insert_returning($options) if $options->{returning};

  return $sql, @bind;
}

sub _order_by {
  my ($self, $arg) = @_;

  # Legacy
  return $self->SUPER::_order_by($arg)
    if ref $arg ne 'HASH'
    or grep {/^-(?:desc|asc)/i} keys %$arg;

  return $self->_pg_parse($arg);
}

sub _pg_parse {
  my ($self, $options) = @_;

  # GROUP BY
  my $sql = '';
  my @bind;
  if (defined(my $group = $options->{group_by})) {
    $self->_SWITCH_refkind(
      $group => {
        SCALARREF => sub { $sql .= $self->_sqlcase(' group by ') . $$group }
      }
    );
  }

  # ORDER BY
  $sql .= $self->_order_by($options->{order_by})
    if defined $options->{order_by};

  # LIMIT
  if (defined $options->{limit}) {
    $sql .= $self->_sqlcase(' limit ') . '?';
    push @bind, $options->{limit};
  }

  # OFFSET
  if (defined $options->{offset}) {
    $sql .= $self->_sqlcase(' offset ') . '?';
    push @bind, $options->{offset};
  }

  # FOR
  if (defined(my $for = $options->{for})) {
    $self->_SWITCH_refkind(
      $for => {
        SCALARREF => sub { $sql .= $self->_sqlcase(' for ') . $$for }
      }
    );
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

=head1 INSERT

Additional C<INSERT> query features.

=head2 ON CONFLICT

The C<on_conflict> option can be used to generate C<INSERT> queries with
C<ON CONFLICT> clauses. So far only scalar references to pass literal SQL and
array reference references to pass literal SQL with bind values are supported.

  # "insert into some_table (foo) values ('bar') on conflict do nothing"
  $abstract->insert(
    'some_table', {foo => 'bar'}, {on_conflict => \'do nothing'});

This includes operations commonly referred to as C<upsert>.

  # "insert into some_table (foo) values ('bar')
  #  on conflict (foo) do update set foo = 'baz'"
  $abstract->insert('some_table', {foo => 'bar'}, {
    on_conflict => \['(foo) do update set foo = ?', 'baz']
  });

=head1 SELECT

Additional C<SELECT> query features.

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

=head2 GROUP BY

The C<group_by> option can be used to generate C<SELECT> queries with
C<GROUP BY> clauses. So far only scalar references to pass literal SQL are
supported.

  # "select * from some_table group by foo, bar"
  $abstract->select('some_table', undef, undef, {group_by => \'foo, bar'});

=head2 FOR

The C<for> option can be used to generate C<SELECT> queries with C<FOR> clauses.
So far only scalar references to pass literal SQL are supported.

  # "select * from some_table for update skip locked"
  $abstract->select('some_table', undef, undef, {for => \'update skip locked'});

=head1 METHODS

L<SQL::Abstract::Pg> inherits all methods from L<SQL::Abstract>.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<http://mojolicious.org>.

=cut
