package Mojo::Pg::Results;
use Mojo::Base -base;

use Mojo::Collection;
use Mojo::Util 'tablify';

has [qw(db sth)];

sub DESTROY {
  my $self = shift;
  if ((my $db = $self->db) && (my $sth = $self->sth)) { $db->_enqueue($sth) }
}

sub array { shift->sth->fetchrow_arrayref }

sub arrays { Mojo::Collection->new(@{shift->sth->fetchall_arrayref}) }

sub columns { shift->sth->{NAME} }

sub hash { shift->sth->fetchrow_hashref }

sub hashes { Mojo::Collection->new(@{shift->sth->fetchall_arrayref({})}) }

sub rows { shift->sth->rows }

sub text { tablify shift->arrays }

1;

=encoding utf8

=head1 NAME

Mojo::Pg::Results - Results

=head1 SYNOPSIS

  use Mojo::Pg::Results;

  my $results = Mojo::Pg::Results->new(db => $db, sth => $sth);

=head1 DESCRIPTION

L<Mojo::Pg::Results> is a container for statement handles used by
L<Mojo::Pg::Database>.

=head1 ATTRIBUTES

L<Mojo::Pg::Results> implements the following attributes.

=head2 db

  my $db   = $results->db;
  $results = $results->db(Mojo::Pg::Database->new);

L<Mojo::Pg::Database> object these results belong to.

=head2 sth

  my $sth  = $results->sth;
  $results = $results->sth($sth);

Statement handle results are fetched from.

=head1 METHODS

L<Mojo::Pg::Results> inherits all methods from L<Mojo::Base> and implements
the following new ones.

=head2 array

  my $array = $results->array;

Fetch one row and return it as an array reference.

=head2 arrays

  my $collection = $results->arrays;

Fetch all rows and return them as a L<Mojo::Collection> object containing
array references.

=head2 columns

  my $columns = $results->columns;

Return column names as an array reference.

=head2 hash

  my $hash = $results->hash;

Fetch one row and return it as a hash reference.

=head2 hashes

  my $collection = $results->hashes;

Fetch all rows and return them as a L<Mojo::Collection> object containing hash
references.

=head2 rows

  my $num = $results->rows;

Number of rows.

=head2 text

  my $text = $results->text;

Fetch all rows and turn them into a table with L<Mojo::Util/"tablify">.

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
