package Blog::Model::Posts;
use Mojo::Base -base;

has 'pg';

sub add {
  my ($self, $title, $body) = @_;
  my $sql = 'insert into posts (title, body) values (?, ?) returning id';
  return $self->pg->db->query($sql, $title, $body)->hash->{id};
}

sub all { shift->pg->db->query('select * from posts')->hashes->each }

sub find {
  my ($self, $id) = @_;
  return $self->pg->db->query('select * from posts where id = ?', $id)->hash;
}

1;
