BEGIN;

DROP TABLE IF EXISTS kvs;
DROP FUNCTION IF EXISTS x_get(in_k TEXT);
DROP FUNCTION IF EXISTS x_set(in_k TEXT, in_v TEXT);
DROP FUNCTION IF EXISTS x_lpush(in_k TEXT, in_v TEXT);
DROP FUNCTION IF EXISTS x_rpush(in_k TEXT, in_v TEXT);
DROP FUNCTION IF EXISTS x_lpop(in_k TEXT);
DROP FUNCTION IF EXISTS x_rpop(in_k TEXT);

CREATE TABLE kvs (
    k TEXT PRIMARY KEY,
    v TEXT,
    a TEXT[],
    expiration TIMESTAMP WITHOUT TIME ZONE,
    CHECK(v IS NOT NULL OR a IS NOT NULL)
);

CREATE FUNCTION x_get(in_k TEXT) RETURNS TEXT AS $$
DECLARE
    t_row kvs%ROWTYPE;
BEGIN
    SELECT *
    INTO t_row
    FROM kvs
    WHERE k = in_k;

    IF t_row.expiration < NOW() THEN
        RETURN NULL;
    ELSE
        RETURN t_row.v;
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_set(in_k TEXT, in_v TEXT) RETURNS VOID AS $$
DECLARE
    c INTEGER;
BEGIN
-- Lame to do the query every time, but for now...
    SELECT COUNT(k)
    INTO c
    FROM kvs
    WHERE k = in_k;
    IF c > 0 THEN
       UPDATE kvs
       SET v = in_v,
           expiration = NULL
       WHERE k = in_k;
    ELSE
       INSERT INTO kvs (k, v) VALUES (in_k, in_v);
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_lpush(in_k TEXT, in_v TEXT) RETURNS VOID AS $$
DECLARE
    c INTEGER;
BEGIN
-- Lame to do the query every time, but for now...
    SELECT COUNT(k)
    INTO c
    FROM kvs
    WHERE k = in_k;
    IF c > 0 THEN
       UPDATE kvs
       SET a = array[in_v] || CASE
                   WHEN (a IS NULL) THEN array[]::text[]
                   ELSE a
               END,
           v = NULL
       WHERE k = in_k;
    ELSE
       INSERT INTO kvs (k, a) VALUES (in_k, array[in_v]);
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_rpush(in_k TEXT, in_v TEXT) RETURNS VOID AS $$
DECLARE
    c INTEGER;
BEGIN
-- Lame to do the query every time, but for now...
    SELECT COUNT(k)
    INTO c
    FROM kvs
    WHERE k = in_k;
    IF c > 0 THEN
       UPDATE kvs
       SET a = CASE
                   WHEN (a IS NULL) THEN array[]::text[]
                   ELSE a
               END || array[in_v],
           v = NULL
       WHERE k = in_k;
    ELSE
       INSERT INTO kvs (k, a) VALUES (in_k, array[in_v]);
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_lpop(in_k TEXT) RETURNS TEXT AS $$
DECLARE
    t_row kvs%ROWTYPE;
    a_len INTEGER;
BEGIN
    -- TODO: test expiration
    SELECT *
    INTO t_row
    FROM kvs
    WHERE k = in_k;

    a_len := array_length(t_row.a, 1);

    IF a_len > 1 THEN
        UPDATE kvs
        SET a = a[2:a_len]
        WHERE k = in_k;
    ELSE
        DELETE FROM kvs
        WHERE k = in_k;
    END IF;

    RETURN t_row.a[1];
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_rpop(in_k TEXT) RETURNS TEXT AS $$
DECLARE
    t_row kvs%ROWTYPE;
    a_len INTEGER;
BEGIN
    -- TODO: test expiration
    SELECT *
    INTO t_row
    FROM kvs
    WHERE k = in_k;

    a_len := array_length(t_row.a, 1);

    IF a_len > 1 THEN
        UPDATE kvs
        SET a = a[1:a_len - 1]
        WHERE k = in_k;
    ELSE
        DELETE FROM kvs
        WHERE k = in_k;
    END IF;

    RETURN t_row.a[a_len];
END
$$ LANGUAGE plpgsql;

COMMIT;

-- "Tests"
BEGIN;

DROP TABLE IF EXISTS tests;
CREATE TABLE tests (
    description TEXT NOT NULL PRIMARY KEY,
    test_result BOOLEAN NOT NULL
);

SELECT x_set('a', 'b');

INSERT INTO tests (description, test_result)
VALUES ('getting a valid key', x_get('a') = 'b');

INSERT INTO tests (description, test_result)
VALUES ('getting an invalid key', x_get('c') IS NULL);

SELECT x_lpush('r', 'foo');
SELECT x_rpush('r', 'bar');

INSERT INTO tests (description, test_result)
VALUES ('lpop once', x_lpop('r') = 'foo');

INSERT INTO tests (description, test_result)
VALUES ('lpop twice', x_lpop('r') = 'bar');

INSERT INTO tests (description, test_result)
VALUES ('lpop thrice', x_lpop('r') IS NULL);

SELECT x_lpush('r', 'foo');
SELECT x_rpush('r', 'bar');

INSERT INTO tests (description, test_result)
VALUES ('rpop once', x_rpop('r') = 'bar');

INSERT INTO tests (description, test_result)
VALUES ('rpop twice', x_rpop('r') = 'foo');

INSERT INTO tests (description, test_result)
VALUES ('rpop thrice', x_rpop('r') IS NULL);

END;