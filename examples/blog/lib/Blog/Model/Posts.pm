package Blog::Model::Posts;
use Mojo::Base -base;

has 'pg';

sub add {
  my ($self, $post) = @_;
  my $sql = 'insert into posts (title, body) values (?, ?) returning id';
  return $self->pg->db->query($sql, $post->{title}, $post->{body})->hash->{id};
}

sub all { shift->pg->db->query('select * from posts')->hashes->to_array }

sub find {
  my ($self, $id) = @_;
  return $self->pg->db->query('select * from posts where id = ?', $id)->hash;
}

sub remove { shift->pg->db->query('delete from posts where id = ?', shift) }

sub save {
  my ($self, $id, $post) = @_;
  my $sql = 'update posts set title = ?, body = ? where id = ?';
  $self->pg->db->query($sql, $post->{title}, $post->{body}, $id);
}

1;
