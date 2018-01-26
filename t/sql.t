use Mojo::Base -strict;

use Test::More;
use Mojo::Pg;

# Basics
my $pg       = Mojo::Pg->new;
my $abstract = $pg->abstract;
is_deeply [$abstract->select('foo', '*')], ['SELECT * FROM "foo"'],
  'right query';

# ORDER BY
my @sql = $abstract->select('foo', '*', {bar => 'baz'}, {-desc => 'yada'});
is_deeply \@sql,
  ['SELECT * FROM "foo" WHERE ( "bar" = ? ) ORDER BY "yada" DESC', 'baz'],
  'right query';
@sql = $abstract->select('foo', '*', {bar => 'baz'},
  {order_by => {-desc => 'yada'}});
is_deeply \@sql,
  ['SELECT * FROM "foo" WHERE ( "bar" = ? ) ORDER BY "yada" DESC', 'baz'],
  'right query';

# LIMIT/OFFSET
@sql = $abstract->select('foo', '*', undef, {limit => 10, offset => 5});
is_deeply \@sql, ['SELECT * FROM "foo" LIMIT ? OFFSET ?', 10, 5], 'right query';

# GROUP BY
@sql = $abstract->select('foo', '*', undef, {group_by => \'bar, baz'});
is_deeply \@sql, ['SELECT * FROM "foo" GROUP BY bar, baz'], 'right query';

# GROUP BY (unsupported value)
eval { $abstract->select('foo', '*', undef, {group_by => []}) };
like $@, qr/Unsupported group_by value "ARRAY/, 'right error';

# FOR
@sql = $abstract->select('foo', '*', undef, {for => \'update skip locked'});
is_deeply \@sql, ['SELECT * FROM "foo" FOR update skip locked'], 'right query';

# FOR (unsupported value)
eval { $abstract->select('foo', '*', undef, {for => []}) };
like $@, qr/Unsupported for value "ARRAY/, 'right error';

done_testing();
