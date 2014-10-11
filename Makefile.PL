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
    requires  => {perl => '5.010001'},
    resources => {
      license    => 'http://www.opensource.org/licenses/artistic-license-2.0',
      homepage   => 'http://mojolicio.us',
      bugtracker => 'https://github.com/kraih/mojo-pg/issues',
      repository => 'https://github.com/kraih/mojo-pg.git',
      x_IRC      => 'irc://irc.perl.org/#mojo'
    },
    no_index => {directory => ['t']}
  },
  PREREQ_PM => {Mojolicious => '5.49', 'DBD::Pg' => 0},
  test      => {TESTS       => 't/*.t t/*/*.t'}
);
