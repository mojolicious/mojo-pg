package Mojo::Pg::Migrations;
use Mojo::Base -base;

use Carp qw(croak);
use Mojo::File qw(path);
use Mojo::Loader qw(data_section);
use Mojo::Util qw(decode);

use constant DEBUG => $ENV{MOJO_MIGRATIONS_DEBUG} || 0;

has name => 'migrations';
has pg => undef, weak => 1;

sub active { $_[0]->_active($_[0]->pg->db) }

sub from_data {
  my ($self, $class, $name) = @_;
  return $self->from_string(data_section($class //= caller, $name // $self->name));
}

sub from_dir {
  my ($self, $dir) = @_;

  my $migrations = $self->{migrations} = {up => {}, down => {}};
  for my $file (path($dir)->list_tree({max_depth => 2})->each) {
    next unless my ($way)     = ($file->basename          =~ /^(up|down)\.sql$/);
    next unless my ($version) = ($file->dirname->basename =~ /^(\d+)$/);
    $migrations->{$way}{$version} = decode 'UTF-8', $file->slurp;
  }

  return $self;
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
  return $self if $self->_active($db) == $target;

  # Lock migrations table and check version again
  $db->query(
    'CREATE TABLE IF NOT EXISTS mojo_migrations (
       name    TEXT PRIMARY KEY,
       version BIGINT NOT NULL CHECK (version >= 0)
     )'
  );
  my $tx = $db->begin;
  $db->query('LOCK TABLE mojo_migrations IN EXCLUSIVE MODE');
  return $self if (my $active = $self->_active($db)) == $target;

  # Newer version
  croak "Active version $active is greater than the latest version $latest" if $active > $latest;

  my $sql = $self->sql_for($active, $target);
  warn "-- Migrate ($active -> $target)\n$sql\n" if DEBUG;
  $sql .= ';INSERT INTO mojo_migrations (name, version) VALUES ($2, $1)';
  $sql .= ' ON CONFLICT (name) DO UPDATE SET version = $1;';
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
  my ($self, $db) = @_;

  my $name = $self->name;
  my $results;
  {
    local $db->dbh->{RaiseError} = 0;
    my $sql = 'SELECT version FROM mojo_migrations WHERE name = $1';
    $results = $db->query($sql, $name);
  };
  if (my $next = $results->array) { return $next->[0] || 0 }
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

L<Mojo::Pg::Migrations> is used by L<Mojo::Pg> to allow database schemas to evolve easily over time. A migration file
is just a collection of sql blocks, with one or more statements, separated by comments of the form C<-- VERSION
UP/DOWN>.

  -- 1 up
  CREATE TABLE messages (message TEXT);
  INSERT INTO messages VALUES ('I ♥ Mojolicious!');
  -- 1 down
  DROP TABLE messages;

  -- 2 up (...you can comment freely here...)
  CREATE TABLE stuff (whatever INT);
  -- 2 down
  DROP TABLE stuff;

The idea is to let you migrate from any version, to any version, up and down. Migrations are very safe, because they
are performed in transactions and only one can be performed at a time. If a single statement fails, the whole migration
will fail and get rolled back. Every set of migrations has a L</"name">, which is stored together with the currently
active version in an automatically created table named C<mojo_migrations>.

=head1 ATTRIBUTES

L<Mojo::Pg::Migrations> implements the following attributes.

=head2 name

  my $name    = $migrations->name;
  $migrations = $migrations->name('foo');

Name for this set of migrations, defaults to C<migrations>.

=head2 pg

  my $pg      = $migrations->pg;
  $migrations = $migrations->pg(Mojo::Pg->new);

L<Mojo::Pg> object these migrations belong to. Note that this attribute is weakened.

=head1 METHODS

L<Mojo::Pg::Migrations> inherits all methods from L<Mojo::Base> and implements the following new ones.

=head2 active

  my $version = $migrations->active;

Currently active version.

=head2 from_data

  $migrations = $migrations->from_data;
  $migrations = $migrations->from_data('main');
  $migrations = $migrations->from_data('main', 'file_name');

Extract migrations from a file in the DATA section of a class with L<Mojo::Loader/"data_section">, defaults to using
the caller class and L</"name">.

  __DATA__
  @@ migrations
  -- 1 up
  CREATE TABLE messages (message TEXT);
  INSERT INTO messages VALUES ('I ♥ Mojolicious!');
  -- 1 down
  DROP TABLE messages;

=head2 from_dir

  $migrations = $migrations->from_dir('/home/sri/migrations');

Extract migrations from a directory tree where each versioned migration is in a directory, named for the version, and
each migration has one or both of the files named C<up.sql> or C<down.sql>.

  migrations/1/up.sql
  migrations/1/down.sql
  migrations/2/up.sql
  migrations/3/up.sql
  migrations/3/down.sql

=head2 from_file

  $migrations = $migrations->from_file('/home/sri/migrations.sql');

Extract migrations from a file.

=head2 from_string

  $migrations = $migrations->from_string(
    '-- 1 up
     CREATE TABLE foo (bar INT);
     -- 1 down
     DROP TABLE foo;'
  );

Extract migrations from string.

=head2 latest

  my $version = $migrations->latest;

Latest version available.

=head2 migrate

  $migrations = $migrations->migrate;
  $migrations = $migrations->migrate(3);

Migrate from L</"active"> to a different version, up or down, defaults to using L</"latest">. All version numbers need
to be positive, with version C<0> representing an empty database.

  # Reset database
  $migrations->migrate(0)->migrate;

=head2 sql_for

  my $sql = $migrations->sql_for(5, 10);

Get SQL to migrate from one version to another, up or down.

=head1 DEBUGGING

You can set the C<MOJO_MIGRATIONS_DEBUG> environment variable to get some advanced diagnostics information printed to
C<STDERR>.

  MOJO_MIGRATIONS_DEBUG=1

=head1 SEE ALSO

L<Mojo::Pg>, L<Mojolicious::Guides>, L<https://mojolicious.org>.

=cut
