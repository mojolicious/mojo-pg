use Mojo::Base -strict;

use Test::More;
use Mojo::Pg;

# Basics
my $pg       = Mojo::Pg->new;
my $abstract = $pg->abstract;
is_deeply [$abstract->insert('foo', {bar => 'baz'})],
  ['INSERT INTO "foo" ( "bar") VALUES ( ? )', 'baz'], 'right query';
is_deeply [$abstract->select('foo', '*')], ['SELECT * FROM "foo"'],
  'right query';
is_deeply [$abstract->select(['foo', 'bar', 'baz'])],
  ['SELECT * FROM "foo", "bar", "baz"'], 'right query';

# ON CONFLICT
my @sql
  = $abstract->insert('foo', {bar => 'baz'}, {on_conflict => \'do nothing'});
is_deeply \@sql,
  ['INSERT INTO "foo" ( "bar") VALUES ( ? ) ON CONFLICT do nothing', 'baz'],
  'right query';
@sql = $abstract->insert('foo', {bar => 'baz'}, {on_conflict => undef});
is_deeply \@sql,
  ['INSERT INTO "foo" ( "bar") VALUES ( ? ) ON CONFLICT DO NOTHING', 'baz'],
  'right query';
@sql = $abstract->insert(
  'foo',
  {bar         => 'baz'},
  {on_conflict => \'do nothing', returning => '*'}
);
my $result = [
  'INSERT INTO "foo" ( "bar") VALUES ( ? ) ON CONFLICT do nothing RETURNING *',
  'baz'
];
is_deeply \@sql, $result, 'right query';
@sql = $abstract->insert(
  'foo',
  {bar         => 'baz'},
  {on_conflict => \['(foo) do update set foo = ?', 'yada']}
);
$result = [
  'INSERT INTO "foo" ( "bar") VALUES ( ? )'
    . ' ON CONFLICT (foo) do update set foo = ?',
  'baz', 'yada'
];
is_deeply \@sql, $result, 'right query';
@sql = $abstract->insert(
  'foo',
  {bar         => 'baz'},
  {on_conflict => [foo => {foo => 'yada'}]}
);
$result = [
  'INSERT INTO "foo" ( "bar") VALUES ( ? )'
    . ' ON CONFLICT ("foo") DO UPDATE SET "foo" = ?',
  'baz', 'yada'
];
is_deeply \@sql, $result, 'right query';
@sql = $abstract->insert(
  'foo',
  {bar         => 'baz'},
  {on_conflict => [['foo', 'bar'] => {foo => 'yada'}]}
);
$result = [
  'INSERT INTO "foo" ( "bar") VALUES ( ? )'
    . ' ON CONFLICT ("foo", "bar") DO UPDATE SET "foo" = ?',
  'baz', 'yada'
];
is_deeply \@sql, $result, 'right query';

# ON CONFLICT (unsupported value)
eval { $abstract->insert('foo', {bar => 'baz'}, {on_conflict => [[], []]}) };
like $@, qr/on_conflict value must be in the form \[\$target, \\\%set\]/,
  'right error';
eval { $abstract->insert('foo', {bar => 'baz'}, {on_conflict => {}}) };
like $@, qr/HASHREF/, 'right error';

# ORDER BY
@sql = $abstract->select('foo', '*', {bar => 'baz'}, {-desc => 'yada'});
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
@sql = $abstract->select('foo', '*', undef, {group_by => ['bar', 'baz']});
is_deeply \@sql, ['SELECT * FROM "foo" GROUP BY "bar", "baz"'], 'right query';

# HAVING
@sql = $abstract->select('foo', '*', undef,
  {group_by => ['bar'], having => {baz => 'yada'}});
is_deeply \@sql,
  ['SELECT * FROM "foo" GROUP BY "bar" HAVING "baz" = ?', 'yada'],
  'right query';
@sql = $abstract->select(
  'foo', '*',
  {bar => {'>' => 'baz'}},
  {group_by => ['bar'], having => {baz => {'<' => 'bar'}}}
);
$result = [
  'SELECT * FROM "foo" WHERE ( "bar" > ? ) GROUP BY "bar" HAVING "baz" < ?',
  'baz', 'bar'
];
is_deeply \@sql, $result, 'right query';

# GROUP BY (unsupported value)
eval { $abstract->select('foo', '*', undef, {group_by => {}}) };
like $@, qr/HASHREF/, 'right error';

# FOR
@sql = $abstract->select('foo', '*', undef, {for => 'update'});
is_deeply \@sql, ['SELECT * FROM "foo" FOR UPDATE'], 'right query';
@sql = $abstract->select('foo', '*', undef, {for => \'update skip locked'});
is_deeply \@sql, ['SELECT * FROM "foo" FOR update skip locked'], 'right query';

# FOR (unsupported value)
eval { $abstract->select('foo', '*', undef, {for => 'update skip locked'}) };
like $@, qr/for value "update skip locked" is not allowed/, 'right error';
eval { $abstract->select('foo', '*', undef, {for => []}) };
like $@, qr/ARRAYREF/, 'right error';

# AS
@sql = $abstract->select('foo', ['bar', [bar => 'baz'], 'yada']);
is_deeply \@sql, ['SELECT "bar", "bar" AS "baz", "yada" FROM "foo"'],
  'right query';
@sql = $abstract->select('foo',
  ['bar', \'extract(epoch from baz) as baz', 'yada']);
is_deeply \@sql,
  ['SELECT "bar", extract(epoch from baz) as baz, "yada" FROM "foo"'],
  'right query';
@sql = $abstract->select('foo', ['bar', \['? as baz', 'test'], 'yada']);
is_deeply \@sql, ['SELECT "bar", ? as baz, "yada" FROM "foo"', 'test'],
  'right query';

# AS (unsupported value)
eval { $abstract->select('foo', [[]]) };
like $@, qr/field alias must be in the form \[\$name => \$alias\]/,
  'right error';

# JSON
@sql = $abstract->update('foo', {bar => {-json => [1, 2, 3]}});
is_deeply \@sql, ['UPDATE "foo" SET "bar" = ?', {json => [1, 2, 3]}],
  'right query';
@sql = $abstract->select('foo', '*', {bar => {'=' => {-json => [1, 2, 3]}}});
is_deeply \@sql,
  ['SELECT * FROM "foo" WHERE ( "bar" = ? )', {json => [1, 2, 3]}],
  'right query';

# JOIN
@sql = $abstract->select(['foo', ['bar', foo_id => 'id']]);
is_deeply \@sql,
  ['SELECT * FROM "foo" JOIN "bar" ON ("bar"."foo_id" = "foo"."id")'],
  'right query';
@sql = $abstract->select(['foo', ['bar', 'foo.id' => 'bar.foo_id']]);
is_deeply \@sql,
  ['SELECT * FROM "foo" JOIN "bar" ON ("foo"."id" = "bar"."foo_id")'],
  'right query';
@sql
  = $abstract->select([
  'foo', ['bar', 'foo.id' => 'bar.foo_id', 'foo.id2' => 'bar.foo_id2']
  ]);
is_deeply \@sql,
  [   'SELECT * FROM "foo" JOIN "bar" ON ("foo"."id" = "bar"."foo_id"'
    . ' AND "foo"."id2" = "bar"."foo_id2"' . ')'
  ], 'right query';
@sql
  = $abstract->select(['foo', ['bar', foo_id => 'id'], ['baz', foo_id => 'id']
  ]);
$result
  = [ 'SELECT * FROM "foo"'
    . ' JOIN "bar" ON ("bar"."foo_id" = "foo"."id")'
    . ' JOIN "baz" ON ("baz"."foo_id" = "foo"."id")'
  ];
is_deeply \@sql, $result, 'right query';
@sql = $abstract->select(['foo', [-left => 'bar', foo_id => 'id']]);
is_deeply \@sql,
  ['SELECT * FROM "foo" LEFT JOIN "bar" ON ("bar"."foo_id" = "foo"."id")'],
  'right query';
@sql = $abstract->select(['foo', [-right => 'bar', foo_id => 'id']]);
is_deeply \@sql,
  ['SELECT * FROM "foo" RIGHT JOIN "bar" ON ("bar"."foo_id" = "foo"."id")'],
  'right query';
@sql = $abstract->select(['foo', [-inner => 'bar', foo_id => 'id']]);
is_deeply \@sql,
  ['SELECT * FROM "foo" INNER JOIN "bar" ON ("bar"."foo_id" = "foo"."id")'],
  'right query';
@sql
  = $abstract->select([
  'foo', [-left => 'bar', foo_id => 'id', foo_id2 => 'id2', foo_id3 => 'id3']
  ]);
is_deeply \@sql,
  [   'SELECT * FROM "foo" LEFT JOIN "bar" ON ("bar"."foo_id" = "foo"."id"'
    . ' AND "bar"."foo_id2" = "foo"."id2"'
    . ' AND "bar"."foo_id3" = "foo"."id3"' . ')'
  ], 'right query';

# JOIN (unsupported value)
eval { $abstract->select(['foo', []]) };
like $@, qr/join must be in the form \[\$table, \$fk => \$pk\]/, 'right error';

done_testing();
