package Blog::Model::Posts;
use Mojo::Base -base, -signatures;

has 'pg';

sub add ($self, $post) {
  return $self->pg->db->insert('posts', $post, {returning => 'id'})->hash->{id};
}

sub all ($self) {
  return $self->pg->db->select('posts')->hashes->to_array;
}

sub find ($self, $id) {
  return $self->pg->db->select('posts', '*', {id => $id})->hash;
}

sub remove ($self, $id) {
  $self->pg->db->delete('posts', {id => $id});
}

sub save ($self, $id, $post) {
  $self->pg->db->update('posts', $post, {id => $id});
}

1;
