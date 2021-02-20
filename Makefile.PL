use 5.016;

use strict;
use warnings;
use utf8;

use ExtUtils::MakeMaker;

WriteMakefile(
  NAME         => 'Mojo::Pg',
  VERSION_FROM => 'lib/Mojo/Pg.pm',
  ABSTRACT     => 'Mojolicious ♥ PostgreSQL',
  AUTHOR       => 'Sebastian Riedel <sri@cpan.org>',
  LICENSE      => 'artistic_2',
  META_MERGE   => {
    dynamic_config => 0,
    'meta-spec'    => {version   => 2},
    no_index       => {directory => ['examples', 't']},
    prereqs        => {runtime   => {requires => {perl => '5.016'}}},
    resources      => {
      bugtracker => {web => 'https://github.com/mojolicious/mojo-pg/issues'},
      homepage   => 'https://mojolicious.org',
      license    => ['http://www.opensource.org/licenses/artistic-license-2.0'],
      repository => {
        type => 'git',
        url  => 'https://github.com/mojolicious/mojo-pg.git',
        web  => 'https://github.com/mojolicious/mojo-pg',
      },
      x_IRC => {url => 'irc://chat.freenode.net/#mojo', web => 'https://webchat.freenode.net/#mojo'}
    },
  },
  PREREQ_PM => {'DBD::Pg' => 3.007004, Mojolicious => '8.50', 'SQL::Abstract::Pg' => '1.0'},
  test      => {TESTS     => 't/*.t t/*/*.t'}
);
