package Blog::Controller::Posts;
use Mojo::Base 'Mojolicious::Controller';

sub create { shift->stash(post => {}) }

sub edit {
  my $self = shift;
  $self->stash(post => $self->posts->find($self->param('id')));
}

sub index {
  my $self = shift;
  $self->stash(posts => $self->posts->all);
}

sub remove {
  my $self = shift;
  $self->posts->withdraw($self->param('id'));
  $self->redirect_to('posts');
}

sub show {
  my $self = shift;
  $self->stash(post => $self->posts->find($self->param('id')));
}

sub store {
  my $self = shift;

  my $validation = $self->_validation;
  return $self->render('posts/create', post => {}) if $validation->has_error;

  my $id = $self->posts->publish($validation->output);
  $self->redirect_to('show_post', id => $id);
}

sub update {
  my $self = shift;

  my $validation = $self->_validation;
  return $self->render('posts/edit', post => {}) if $validation->has_error;

  my $id = $self->param('id');
  $self->posts->revise($id, $validation->output);
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
