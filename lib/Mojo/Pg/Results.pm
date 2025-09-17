package Mojo::Pg::Results;
use Mojo::Base -base;

use Mojo::Collection;
use Mojo::JSON qw(from_json);
use Mojo::Util qw(tablify);

has [qw(db sth)];

sub DESTROY {
  my $self = shift;
  return unless my $sth = $self->{sth};
  $self->finish unless --$sth->{private_mojo_results};
}

sub array { ($_[0]->_expand($_[0]->sth->fetchrow_arrayref))[0] }

sub arrays { _collect($_[0]->_expand(@{$_[0]->sth->fetchall_arrayref})) }

sub columns { shift->sth->{NAME} }

sub hash { ($_[0]->_expand($_[0]->sth->fetchrow_hashref))[0] }

sub expand { ++$_[0]{expand} and return $_[0] }

sub finish { $_[0]->db->_finish_when_safe($_[0]->sth) }

sub hashes { _collect($_[0]->_expand(@{$_[0]->sth->fetchall_arrayref({})})) }

sub new {
  my $self = shift->SUPER::new(@_);
  ($self->{sth}{private_mojo_results} //= 0)++;
  return $self;
}

sub rows { shift->sth->rows }

sub text { tablify shift->arrays }

sub _collect { Mojo::Collection->new(@_) }

sub _expand {
  my ($self, @rows) = @_;

  return @rows unless $self->{expand} && $rows[0];
  my ($idx, $name) = @$self{qw(idx name)};
  unless ($idx) {
    my $types = $self->sth->{pg_type};
    my @idx   = grep { $types->[$_] eq 'json' || $types->[$_] eq 'jsonb' } 0 .. $#$types;
    ($idx, $name) = @$self{qw(idx name)} = (\@idx, [@{$self->columns}[@idx]]);
  }

  return @rows unless @$idx;
  if (ref $rows[0] eq 'HASH') {
    for my $r (@rows) { $r->{$_} && ($r->{$_} = from_json $r->{$_}) for @$name }
  }
  else {
    for my $r (@rows) { $r->[$_] && ($r->[$_] = from_json $r->[$_]) for @$idx }
  }

  return @rows;
}

1;

=encoding utf8

=head1 NAME

Mojo::Pg::Results - Results

=head1 SYNOPSIS

  use Mojo::Pg::Results;

  my $results = Mojo::Pg::Results->new(sth => $sth);
  $results->hashes->map(sub { $_->{foo} })->shuffle->join("\n")->say;

=head1 DESCRIPTION

L<Mojo::Pg::Results> is a container for L<DBD::Pg> statement handles used by L<Mojo::Pg::Database>.

=head1 ATTRIBUTES

L<Mojo::Pg::Results> implements the following attributes.

=head2 db

  my $db   = $results->db;
  $results = $results->db(Mojo::Pg::Database->new);

L<Mojo::Pg::Database> object these results belong to.

=head2 sth

  my $sth  = $results->sth;
  $results = $results->sth($sth);

L<DBD::Pg> statement handle results are fetched from.

=head1 METHODS

L<Mojo::Pg::Results> inherits all methods from L<Mojo::Base> and implements the following new ones.

=head2 array

  my $array = $results->array;

Fetch one row from L</"sth"> and return it as an array reference.

=head2 arrays

  my $collection = $results->arrays;

Fetch all rows from L</"sth"> and return them as a L<Mojo::Collection> object containing array references.

  # Process all rows at once
  say $results->arrays->reduce(sub { $a + $b->[3] }, 0);

=head2 columns

  my $columns = $results->columns;

Return column names as an array reference.

  # Names of all columns
  say for @{$results->columns};

=head2 expand

  $results = $results->expand;

Decode C<json> and C<jsonb> fields automatically to Perl values for all rows.

  # Expand JSON
  $results->expand->hashes->map(sub { $_->{foo}{bar} })->join("\n")->say;

=head2 finish

  $results->finish;

Indicate that you are finished with L</"sth"> and will not be fetching all the remaining rows.

=head2 hash

  my $hash = $results->hash;

Fetch one row from L</"sth"> and return it as a hash reference.

=head2 hashes

  my $collection = $results->hashes;

Fetch all rows from L</"sth"> and return them as a L<Mojo::Collection> object containing hash references.

  # Process all rows at once
  say $results->hashes->reduce(sub { $a + $b->{money} }, 0);

=head2 new

  my $results = Mojo::Pg::Results->new;
  my $results = Mojo::Pg::Results->new(sth => $sth);
  my $results = Mojo::Pg::Results->new({sth => $sth});

Construct a new L<Mojo::Pg::Results> object.

=head2 rows

  my $num = $results->rows;

Number of rows.

=head2 text

  my $text = $results->text;

Fetch all rows from L</"sth"> and turn them into a table with L<Mojo::Util/"tablify">.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<https://mojolicious.org>.

=cut
