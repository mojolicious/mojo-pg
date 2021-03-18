package Blog::Controller::Posts;
use Mojo::Base 'Mojolicious::Controller', -signatures;

sub create ($self) {
  $self->render(post => {});
}

sub edit ($self) {
  $self->render(post => $self->posts->find($self->param('id')));
}

sub index ($self) {
  $self->render(posts => $self->posts->all);
}

sub remove ($self) {
  $self->posts->remove($self->param('id'));
  $self->redirect_to('posts');
}

sub show ($self) {
  $self->render(post => $self->posts->find($self->param('id')));
}

sub store ($self) {
  my $v = $self->_validation;
  return $self->render(action => 'create', post => {}) if $v->has_error;

  my $id = $self->posts->add({title => $v->param('title'), body => $v->param('body')});
  $self->redirect_to('show_post', id => $id);
}

sub update ($self) {
  my $v = $self->_validation;
  return $self->render(action => 'edit', post => {}) if $v->has_error;

  my $id = $self->param('id');
  $self->posts->save($id, {title => $v->param('title'), body => $v->param('body')});
  $self->redirect_to('show_post', id => $id);
}

sub _validation ($self) {
  my $v = $self->validation;
  $v->required('title', 'not_empty');
  $v->required('body',  'not_empty');
  return $v;
}

1;
