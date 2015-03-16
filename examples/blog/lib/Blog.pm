package Blog;
use Mojo::Base 'Mojolicious';

use Blog::Model::Posts;
use Mojo::Pg;

sub startup {
  my $self = shift;

  # Configuration
  $self->plugin('Config');
  $self->secrets($self->config('secrets'));

  # Model
  $self->helper(pg => sub { state $pg = Mojo::Pg->new(shift->config('pg')) });
  $self->helper(
    posts => sub { state $posts = Blog::Model::Posts->new(pg => shift->pg) });

  # Migrate to latest version if necessary
  my $path = $self->home->rel_file('migrations/blog.sql');
  $self->pg->migrations->name('blog')->from_file($path)->migrate;

  # Controller
  my $r = $self->routes;
  $r->get('/' => sub { shift->redirect_to('posts') });
  $r->get('/posts')->to('posts#index');
  $r->get('/posts/create')->to('posts#create')->name('create_post');
  $r->post('/posts')->to('posts#store')->name('store_post');
  $r->get('/posts/:id')->to('posts#show')->name('show_post');
  $r->delete('/posts/:id')->to('posts#remove')->name('remove_post');
}

1;
