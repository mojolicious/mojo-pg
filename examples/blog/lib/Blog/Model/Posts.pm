package Blog::Model::Posts;
use Mojo::Base -base;

has 'pg';

sub add {
  my ($self, $post) = @_;
  return $self->pg->db->insert('posts', $post, {returning => 'id'})->hash->{id};
}

sub all { shift->pg->db->select('posts')->hashes->to_array }

sub find {
  my ($self, $id) = @_;
  return $self->pg->db->select('posts', '*', {id => $id})->hash;
}

sub remove {
  my ($self, $id) = @_;
  $self->pg->db->delete('posts', {id => $id});
}

sub save {
  my ($self, $id, $post) = @_;
  $self->pg->db->update('posts', $post, {id => $id});
}

1;
