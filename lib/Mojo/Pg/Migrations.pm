package Mojo::Pg::Migrations;
use Mojo::Base -base;

use Carp 'croak';
use Mojo::File 'path';
use Mojo::Loader 'data_section';
use Mojo::Util 'decode';

use constant DEBUG => $ENV{MOJO_MIGRATIONS_DEBUG} || 0;

has name => 'migrations';
has pg   => undef, weak => 1;

sub active { $_[0]->_active($_[0]->pg->db) }

sub from_data {
  my ($self, $class, $name) = @_;
  return $self->from_string(
    data_section($class //= caller, $name // $self->name));
}

sub from_file { shift->from_string(decode 'UTF-8', path(pop)->slurp) }

sub from_string {
  my ($self, $sql) = @_;

  my ($version, $way);
  my $migrations = $self->{migrations} = {up => {}, down => {}};
  for my $line (split "\n", $sql // '') {
    ($version, $way) = ($1, lc $2) if $line =~ /^\s*--\s*(\d+)\s*(up|down)/i;
    $migrations->{$way}{$version} .= "$line\n" if $version;
  }

  return $self;
}

sub latest {
  (sort { $a <=> $b } keys %{shift->{migrations}{up}})[-1] || 0;
}

sub migrate {
  my ($self, $target) = @_;

  # Unknown version
  my $latest = $self->latest;
  $target //= $latest;
  my ($up, $down) = @{$self->{migrations}}{qw(up down)};
  croak "Version $target has no migration" if $target != 0 && !$up->{$target};

  # Already the right version (make sure migrations table exists)
  my $db = $self->pg->db;
  return $self if $self->_active($db, 1) == $target;

  # Lock migrations table and check version again
  my $tx = $db->begin;
  $db->query('lock table mojo_migrations in exclusive mode');
  return $self if (my $active = $self->_active($db, 1)) == $target;

  # Newer version
  croak "Active version $active is greater than the latest version $latest"
    if $active > $latest;

  my $sql = $self->sql_for($active, $target);
  warn "-- Migrate ($active -> $target)\n$sql\n" if DEBUG;
  $sql .= ';update mojo_migrations set version = $1 where name = $2;';
  $db->query($sql, $target, $self->name) and $tx->commit;

  return $self;
}

sub sql_for {
  my ($self, $from, $to) = @_;

  # Up
  my ($up, $down) = @{$self->{migrations}}{qw(up down)};
  if ($from < $to) {
    my @up = grep { $_ <= $to && $_ > $from } keys %$up;
    return join '', @$up{sort { $a <=> $b } @up};
  }

  # Down
  my @down = grep { $_ > $to && $_ <= $from } keys %$down;
  return join '', @$down{reverse sort { $a <=> $b } @down};
}

sub _active {
  my ($self, $db, $create) = @_;

  my $name = $self->name;
  my $results;
  {
    local $db->dbh->{RaiseError} = 0;
    my $sql = 'select version from mojo_migrations where name = $1';
    $results = $db->query($sql, $name);
  };
  if ((my $next = $results->array) || !$create) { return $next->[0] || 0 }

  $db->query(
    'create table if not exists mojo_migrations (
       name    text primary key,
       version bigint not null check (version >= 0)
     )'
  ) if $results->sth->err;
  $db->query('insert into mojo_migrations values ($1, $2)', $name, 0);

  return 0;
}

1;

=encoding utf8

=head1 NAME

Mojo::Pg::Migrations - Migrations

=head1 SYNOPSIS

  use Mojo::Pg::Migrations;

  my $migrations = Mojo::Pg::Migrations->new(pg => $pg);
  $migrations->from_file('/home/sri/migrations.sql')->migrate;

=head1 DESCRIPTION

L<Mojo::Pg::Migrations> is used by L<Mojo::Pg> to allow database schemas to
evolve easily over time. A migration file is just a collection of sql blocks,
with one or more statements, separated by comments of the form
C<-- VERSION UP/DOWN>.

  -- 1 up
  create table messages (message text);
  insert into messages values ('I ♥ Mojolicious!');
  -- 1 down
  drop table messages;

  -- 2 up (...you can comment freely here...)
  create table stuff (whatever int);
  -- 2 down
  drop table stuff;

The idea is to let you migrate from any version, to any version, up and down.
Migrations are very safe, because they are performed in transactions and only
one can be performed at a time. If a single statement fails, the whole
migration will fail and get rolled back. Every set of migrations has a
L</"name">, which is stored together with the currently active version in an
automatically created table named C<mojo_migrations>.

=head1 ATTRIBUTES

L<Mojo::Pg::Migrations> implements the following attributes.

=head2 name

  my $name    = $migrations->name;
  $migrations = $migrations->name('foo');

Name for this set of migrations, defaults to C<migrations>.

=head2 pg

  my $pg      = $migrations->pg;
  $migrations = $migrations->pg(Mojo::Pg->new);

L<Mojo::Pg> object these migrations belong to. Note that this attribute is
weakened.

=head1 METHODS

L<Mojo::Pg::Migrations> inherits all methods from L<Mojo::Base> and implements
the following new ones.

=head2 active

  my $version = $migrations->active;

Currently active version.

=head2 from_data

  $migrations = $migrations->from_data;
  $migrations = $migrations->from_data('main');
  $migrations = $migrations->from_data('main', 'file_name');

Extract migrations from a file in the DATA section of a class with
L<Mojo::Loader/"data_section">, defaults to using the caller class and
L</"name">.

  __DATA__
  @@ migrations
  -- 1 up
  create table messages (message text);
  insert into messages values ('I ♥ Mojolicious!');
  -- 1 down
  drop table messages;

=head2 from_file

  $migrations = $migrations->from_file('/home/sri/migrations.sql');

Extract migrations from a file.

=head2 from_string

  $migrations = $migrations->from_string(
    '-- 1 up
     create table foo (bar int);
     -- 1 down
     drop table foo;'
  );

Extract migrations from string.

=head2 latest

  my $version = $migrations->latest;

Latest version available.

=head2 migrate

  $migrations = $migrations->migrate;
  $migrations = $migrations->migrate(3);

Migrate from L</"active"> to a different version, up or down, defaults to using
L</"latest">. All version numbers need to be positive, with version C<0>
representing an empty database.

  # Reset database
  $migrations->migrate(0)->migrate;

=head2 sql_for

  my $sql = $migrations->sql_for(5, 10);

Get SQL to migrate from one version to another, up or down.

=head1 DEBUGGING

You can set the C<MOJO_MIGRATIONS_DEBUG> environment variable to get some
advanced diagnostics information printed to C<STDERR>.

  MOJO_MIGRATIONS_DEBUG=1

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<https://mojolicious.org>.

=cut
