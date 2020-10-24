-- 1 up
CREATE TABLE IF NOT EXISTS migration_test_three (baz VARCHAR(255));
-- 1 down
DROP TABLE IF EXISTS migration_test_three;
-- 2 up
INSERT INTO migration_test_three VALUES ('just');
INSERT INTO migration_test_three VALUES ('works ♥');
-- 3 up
-- 4 up
does_not_exist;
