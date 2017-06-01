use Mojo::Base -strict;

use Test::More;

# This test requires a PostgreSQL connection string for an existing database
#
#   TEST_ONLINE=postgres://tester:testing@/test script/blog test
#
plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use Mojo::Pg;
use Mojo::URL;
use Test::Mojo;

# Isolate tests
my $url
  = Mojo::URL->new($ENV{TEST_ONLINE})->query([search_path => 'mojo_blog_test']);
my $pg = Mojo::Pg->new($url);
$pg->db->query('drop schema if exists mojo_blog_test cascade');
$pg->db->query('create schema mojo_blog_test');

# Override configuration for testing
my $t = Test::Mojo->new(Blog => {pg => $url, secrets => ['test_s3cret']});
$t->ua->max_redirects(10);

# No posts yet
$t->get_ok('/')->status_is(200)->text_is('title' => 'Blog')
  ->text_is('body > a' => 'New post')->element_exists_not('h2');

# Create a new post
$t->get_ok('/posts/create')->status_is(200)->text_is('title' => 'New post')
  ->element_exists('form input[name=title]')
  ->element_exists('form textarea[name=body]');
$t->post_ok('/posts' => form => {title => 'Testing', body => 'This is a test.'})
  ->status_is(200)->text_is('title' => 'Testing')->text_is('h2' => 'Testing')
  ->text_like('p' => qr/This is a test/);

# Read the post
$t->get_ok('/')->status_is(200)->text_is('title' => 'Blog')
  ->text_is('h2 a' => 'Testing')->text_like('p' => qr/This is a test/);
$t->get_ok('/posts/1')->status_is(200)->text_is('title' => 'Testing')
  ->text_is('h2' => 'Testing')->text_like('p' => qr/This is a test/)
  ->text_is('body > a' => 'Edit');

# Update the post
$t->get_ok('/posts/1/edit')->status_is(200)->text_is('title' => 'Edit post')
  ->element_exists('form input[name=title][value=Testing]')
  ->text_like('form textarea[name=body]' => qr/This is a test/)
  ->element_exists('form input[value=Remove]');
$t->post_ok(
  '/posts/1?_method=PUT' => form => {title => 'Again', body => 'It works.'})
  ->status_is(200)->text_is('title' => 'Again')->text_is('h2' => 'Again')
  ->text_like('p' => qr/It works/);
$t->get_ok('/posts/1')->status_is(200)->text_is('title' => 'Again')
  ->text_is('h2' => 'Again')->text_like('p' => qr/It works/);

# Delete the post
$t->post_ok('/posts/1?_method=DELETE')->status_is(200)
  ->text_is('title' => 'Blog')->element_exists_not('h2');

# Clean up once we are done
$pg->db->query('drop schema mojo_blog_test cascade');

done_testing();
