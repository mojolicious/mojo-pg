package Blog::Controller::Posts;
use Mojo::Base 'Mojolicious::Controller';

sub create { shift->render(post => {}) }

sub edit {
  my $self = shift;
  $self->render(post => $self->posts->find($self->param('id')));
}

sub index {
  my $self = shift;
  $self->render(posts => $self->posts->all);
}

sub remove {
  my $self = shift;
  $self->posts->remove($self->param('id'));
  $self->redirect_to('posts');
}

sub show {
  my $self = shift;
  $self->render(post => $self->posts->find($self->param('id')));
}

sub store {
  my $self = shift;

  my $validation = $self->_validation;
  return $self->render(action => 'create', post => {})
    if $validation->has_error;

  my $id = $self->posts->add($validation->output);
  $self->redirect_to('show_post', id => $id);
}

sub update {
  my $self = shift;

  my $validation = $self->_validation;
  return $self->render(action => 'edit', post => {}) if $validation->has_error;

  my $id = $self->param('id');
  $self->posts->save($id, $validation->output);
  $self->redirect_to('show_post', id => $id);
}

sub _validation {
  my $self = shift;

  my $validation = $self->validation;
  $validation->required('title');
  $validation->required('body');

  return $validation;
}

1;
