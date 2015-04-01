package Blog::Model::Posts;
use Mojo::Base -base;

has 'pg';

sub all { shift->pg->db->query('select * from posts')->hashes->to_array }

sub find {
  my ($self, $id) = @_;
  return $self->pg->db->query('select * from posts where id = ?', $id)->hash;
}

sub publish {
  my ($self, $post) = @_;
  my $sql = 'insert into posts (title, body) values (?, ?) returning id';
  return $self->pg->db->query($sql, $post->{title}, $post->{body})->hash->{id};
}

sub revise {
  my ($self, $id, $post) = @_;
  my $sql = 'update posts set title = ?, body = ? where id = ?';
  $self->pg->db->query($sql, $post->{title}, $post->{body}, $id);
}

sub withdraw { shift->pg->db->query('delete from posts where id = ?', shift) }

1;
