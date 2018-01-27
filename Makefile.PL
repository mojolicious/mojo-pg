use 5.010001;

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
    'meta-spec'    => {version => 2},
    no_index       => {directory => ['examples', 't']},
    prereqs        => {runtime => {requires => {perl => '5.010001'}}},
    resources      => {
      bugtracker => {web => 'https://github.com/kraih/mojo-pg/issues'},
      homepage   => 'http://mojolicious.org',
      license    => ['http://www.opensource.org/licenses/artistic-license-2.0'],
      repository => {
        type => 'git',
        url  => 'https://github.com/kraih/mojo-pg.git',
        web  => 'https://github.com/kraih/mojo-pg',
      },
      x_IRC => 'irc://irc.perl.org/#mojo'
    },
  },
  PREREQ_PM =>
    {'DBD::Pg' => 3.005001, Mojolicious => '7.53', 'SQL::Abstract' => '1.85'},
  test => {TESTS => 't/*.t t/*/*.t'}
);
