package Mojo::Pg::Results;
use Mojo::Base -base;

use Mojo::Collection;
use Mojo::JSON 'decode_json';
use Mojo::Util 'tablify';

has 'sth';

sub array { ($_[0]->_expand($_[0]->sth->fetchrow_arrayref))[0] }

sub arrays {
  Mojo::Collection->new($_[0]->_expand(@{$_[0]->sth->fetchall_arrayref}));
}

sub columns { shift->sth->{NAME} }

sub hash { ($_[0]->_expand($_[0]->sth->fetchrow_hashref))[0] }

sub expand { ++$_[0]{expand} and return $_[0] }

sub hashes {
  Mojo::Collection->new($_[0]->_expand(@{$_[0]->sth->fetchall_arrayref({})}));
}

sub rows { shift->sth->rows }

sub text { tablify shift->arrays }

sub _expand {
  my ($self, @data) = @_;

  return @data unless $data[0] && $self->{expand};
  my ($idx, $name) = @$self{qw(idx name)};
  unless ($idx) {
    my $types = $self->sth->{pg_type};
    my @idx = grep { $types->[$_] eq 'json' || $types->[$_] eq 'jsonb' }
      0 .. $#$types;
    ($idx, $name) = @$self{qw(idx name)} = (\@idx, [@{$self->columns}[@idx]]);
  }
  return @data unless @$idx;

  for my $data (@data) {
    if (ref $data eq 'HASH') { $data->{$_} and _json($data->{$_}) for @$name }
    else                     { $data->[$_] and _json($data->[$_]) for @$idx }
  }

  return @data;
}

sub _json { $_[0] = decode_json $_[0] }

1;

=encoding utf8

=head1 NAME

Mojo::Pg::Results - Results

=head1 SYNOPSIS

  use Mojo::Pg::Results;

  my $results = Mojo::Pg::Results->new(sth => $sth);
  $results->hashes->map(sub { $_->{foo} })->shuffle->join("\n")->say;

=head1 DESCRIPTION

L<Mojo::Pg::Results> is a container for L<DBD::Pg> statement handles used by
L<Mojo::Pg::Database>.

=head1 ATTRIBUTES

L<Mojo::Pg::Results> implements the following attributes.

=head2 sth

  my $sth  = $results->sth;
  $results = $results->sth($sth);

L<DBD::Pg> statement handle results are fetched from.

=head1 METHODS

L<Mojo::Pg::Results> inherits all methods from L<Mojo::Base> and implements
the following new ones.

=head2 array

  my $array = $results->array;

Fetch next row from L</"sth"> and return it as an array reference.

  # Process one row at a time
  while (my $next = $results->array) {
    say $next->[3];
  }

=head2 arrays

  my $collection = $results->arrays;

Fetch all rows from L</"sth"> and return them as a L<Mojo::Collection> object
containing array references.

  # Process all rows at once
  say $results->arrays->reduce(sub { $a->[3] + $b->[3] });

=head2 columns

  my $columns = $results->columns;

Return column names as an array reference.

=head2 expand

  $results = $results->expand;

Decode C<json> and C<jsonb> fields automatically for all rows.

  # Expand JSON
  $results->expand->hashes->map(sub { $_->{foo}{bar} })->join("\n")->say;

=head2 hash

  my $hash = $results->hash;

Fetch next row from L</"sth"> and return it as a hash reference.

  # Process one row at a time
  while (my $next = $results->hash) {
    say $next->{money};
  }

=head2 hashes

  my $collection = $results->hashes;

Fetch all rows from L</"sth"> and return them as a L<Mojo::Collection> object
containing hash references.

  # Process all rows at once
  say $results->hashes->reduce(sub { $a->{money} + $b->{money} });

=head2 rows

  my $num = $results->rows;

Number of rows.

=head2 text

  my $text = $results->text;

Fetch all rows from L</"sth"> and turn them into a table with
L<Mojo::Util/"tablify">.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
