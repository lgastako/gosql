BEGIN;

DROP TABLE IF EXISTS kvs;
DROP FUNCTION IF EXISTS expired(ts TIMESTAMP WITHOUT TIME ZONE);
DROP FUNCTION IF EXISTS get_row_for_key(in_n INTEGER, in_k TEXT);
DROP FUNCTION IF EXISTS x_exists(in_n INTEGER, in_k TEXT);
DROP FUNCTION IF EXISTS x_get(in_n INTEGER, in_k TEXT);
DROP FUNCTION IF EXISTS x_set(in_n INTEGER, in_k TEXT, in_v TEXT);
DROP FUNCTION IF EXISTS x_del(in_n INTEGER, in_k TEXT);
DROP FUNCTION IF EXISTS x_lpush(in_n INTEGER, in_k TEXT, in_v TEXT);
DROP FUNCTION IF EXISTS x_rpush(in_n INTEGER, in_k TEXT, in_v TEXT);
DROP FUNCTION IF EXISTS x_lpop(in_n INTEGER, in_k TEXT);
DROP FUNCTION IF EXISTS x_rpop(in_n INTEGER, in_k TEXT);
DROP FUNCTION IF EXISTS x_rename(in_n INTEGER, in_old_k TEXT,
                                 in_new_k TEXT);
DROP FUNCTION IF EXISTS x_renamenx(in_n INTEGER, in_old_k TEXT,
                                   in_new_k TEXT);
DROP FUNCTION IF EXISTS x_dbsize(in_n INTEGER);
DROP FUNCTION IF EXISTS x_mget(in_n INTEGER, in_ks TEXT[]);
DROP FUNCTION IF EXISTS x_expire(in_n INTEGER, in_k TEXT,
                                 in_use_by TIMESTAMP WITHOUT TIME ZONE);

CREATE TABLE kvs (
    n INTEGER,
    k TEXT,
    v TEXT,
    a TEXT[],
    use_by TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY(n, k),
    CHECK(v IS NOT NULL OR a IS NOT NULL)
);

CREATE FUNCTION expired(ts TIMESTAMP WITHOUT TIME ZONE) RETURNS BOOLEAN AS $$
BEGIN
    IF ts IS NULL THEN
        RETURN False;
    ELSE
        RETURN ts < NOW();
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_row_for_key(in_n INTEGER, in_k TEXT)
     RETURNS record AS $$
DECLARE
    t_row kvs%ROWTYPE;
BEGIN
    SELECT *
    INTO t_row
    FROM kvs
    WHERE n = in_n
    AND k = in_k
    AND NOT expired(use_by);
    RETURN t_row;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_exists(in_n INTEGER, in_k TEXT) RETURNS BOOLEAN AS $$
DECLARE
    c INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO c
    FROM kvs
    WHERE n = in_n
    AND k = in_k;
    RETURN c > 0;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_get(in_n INTEGER, in_k TEXT) RETURNS TEXT AS $$
DECLARE
    t_row kvs%ROWTYPE;
BEGIN
    t_row := get_row_for_key(in_n, in_k);
    RETURN t_row.v;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_set(in_n INTEGER, in_k TEXT, in_v TEXT)
    RETURNS VOID AS $$
DECLARE
    c INTEGER;
BEGIN
    IF x_exists(in_n, in_k) THEN
       UPDATE kvs
       SET v = in_v,
           use_by = NULL
       WHERE n = in_n
       AND k = in_k;
    ELSE
       INSERT INTO kvs (n, k, v)
       VALUES (in_n, in_k, in_v);
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_del(in_n INTEGER, in_k TEXT) RETURNS VOID AS $$
BEGIN
    DELETE FROM kvs
    WHERE n = in_n
    AND k = in_k;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_lpush(in_n INTEGER, in_k TEXT, in_v TEXT)
    RETURNS VOID AS $$
DECLARE
    c INTEGER;
BEGIN
    IF x_exists(in_n, in_k) THEN
       UPDATE kvs
       SET a = array[in_v] || CASE
                   WHEN (a IS NULL) THEN array[]::text[]
                   ELSE a
               END,
           v = NULL
       WHERE n = in_n
       AND k = in_k;
    ELSE
       INSERT INTO kvs (n, k, a) VALUES (in_n, in_k, array[in_v]);
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_rpush(in_n INTEGER, in_k TEXT, in_v TEXT) RETURNS VOID AS $$
DECLARE
    c INTEGER;
BEGIN
    IF x_exists(in_n, in_k) THEN
       UPDATE kvs
       SET a = CASE
                   WHEN (a IS NULL) THEN array[]::text[]
                   ELSE a
               END || array[in_v],
           v = NULL
       WHERE n = in_n
       AND k = in_k;
    ELSE
       INSERT INTO kvs (n, k, a) VALUES (in_n, in_k, array[in_v]);
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_lpop(in_n INTEGER, in_k TEXT) RETURNS TEXT AS $$
DECLARE
    t_row kvs%ROWTYPE;
    a_len INTEGER;
BEGIN
    t_row := get_row_for_key(in_n, in_k);

    a_len := array_length(t_row.a, 1);

    IF a_len > 1 THEN
        UPDATE kvs
        SET a = a[2:a_len]
        WHERE n = in_n
        AND k = in_k;
    ELSE
        DELETE FROM kvs
        WHERE n = in_n
        AND k = in_k;
    END IF;

    RETURN t_row.a[1];
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_rpop(in_n INTEGER, in_k TEXT) RETURNS TEXT AS $$
DECLARE
    t_row kvs%ROWTYPE;
    a_len INTEGER;
BEGIN
    t_row := get_row_for_key(in_n, in_k);
    a_len := array_length(t_row.a, 1);

    IF a_len > 1 THEN
        UPDATE kvs
        SET a = a[1:a_len - 1]
        WHERE n = in_n
        AND k = in_k;
    ELSE
        DELETE FROM kvs
        WHERE n = in_n
        AND k = in_k;
    END IF;

    RETURN t_row.a[a_len];
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_rename(in_n INTEGER, in_old_k TEXT, in_new_k TEXT)
    RETURNS VOID AS $$
BEGIN
    PERFORM x_del(in_n, in_new_k);

    UPDATE kvs
    SET k = in_new_k
    WHERE n = in_n
    AND k = in_old_k;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_renamenx(in_n INTEGER, in_old_k TEXT, in_new_k TEXT)
    RETURNS BOOLEAN AS $$
DECLARE
    c INTEGER;
BEGIN
    IF NOT x_exists(in_n, in_new_k) THEN
        UPDATE kvs
        SET k = in_new_k
        WHERE n = in_n
        AND k = in_old_k;
        RETURN True;
    ELSE
        RETURN False;
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_dbsize(in_n INTEGER) RETURNS INTEGER AS $$
DECLARE
    c INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO c
    FROM kvs
    WHERE n = in_n;
    RETURN c;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_mget(in_n INTEGER, in_ks TEXT[]) RETURNS TEXT[] AS $$
DECLARE
    t_row kvs%ROWTYPE;
    res TEXT[];
    idx INTEGER;
    len INTEGER;
BEGIN
    len := array_length(in_ks, 1);
    res := array_fill(NULL::TEXT, ARRAY[len]);
    FOR t_row IN SELECT *
                 FROM kvs
                 WHERE n = in_n
                 AND in_ks @> ARRAY[k]
                 AND NOT expired(use_by)
    LOOP
        FOR idx IN 1 .. len
        LOOP
            IF t_row.k = in_ks[idx] THEN
              res[idx] = t_row.v;
            END IF;
        END LOOP;
    END LOOP;
    RETURN res;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_expire(in_n INTEGER, in_k TEXT,
                         in_use_by TIMESTAMP WITHOUT TIME ZONE)
     RETURNS VOID AS $$
BEGIN
    UPDATE kvs
    SET use_by = in_use_by
    WHERE n = in_n
    AND k = in_k;
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

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'a', 'b');

INSERT INTO tests (description, test_result)
VALUES ('existence check for existent key', x_exists(0, 'a') = True);

-------------------
TRUNCATE TABLE kvs;

INSERT INTO tests (description, test_result)
VALUES ('existence check for non-existent key', x_exists(0, 'abc') = False);

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'a', 'b');

INSERT INTO tests (description, test_result)
VALUES ('getting a valid key', x_get(0, 'a') = 'b');

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'a', 'b');
SELECT x_del(0, 'a');

INSERT INTO tests (description, test_result)
VALUES ('deleting an existent key', x_get(0, 'a') IS NULL);

-------------------
TRUNCATE TABLE kvs;

INSERT INTO tests (description, test_result)
VALUES ('getting an invalid key', x_get(0, 'c') IS NULL);

-------------------
TRUNCATE TABLE kvs;

SELECT x_lpush(0, 'r', 'foo');
SELECT x_rpush(0, 'r', 'bar');

INSERT INTO tests (description, test_result)
VALUES ('lpop once', x_lpop(0, 'r') = 'foo');

INSERT INTO tests (description, test_result)
VALUES ('lpop twice', x_lpop(0, 'r') = 'bar');

INSERT INTO tests (description, test_result)
VALUES ('lpop thrice', x_lpop(0, 'r') IS NULL);

-------------------
TRUNCATE TABLE kvs;

SELECT x_lpush(0, 'r', 'foo');
SELECT x_rpush(0, 'r', 'bar');

INSERT INTO tests (description, test_result)
VALUES ('rpop once', x_rpop(0, 'r') = 'bar');

INSERT INTO tests (description, test_result)
VALUES ('rpop twice', x_rpop(0, 'r') = 'foo');

INSERT INTO tests (description, test_result)
VALUES ('rpop thrice', x_rpop(0, 'r') IS NULL);

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'a', 'b');
SELECT x_rename(0, 'a', 'q');

INSERT INTO tests (description, test_result)
VALUES ('rename to non-existent key', x_get(0, 'q') = 'b');

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'aa', 'bb');
SELECT x_set(0, 'bb', 'cc');

SELECT x_rename(0, 'aa', 'bb');

INSERT INTO tests (description, test_result)
VALUES ('rename to existent key', x_get(0, 'bb') = 'bb');

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'a', 'b');
SELECT x_renamenx(0, 'a', 'q');

INSERT INTO tests (description, test_result)
VALUES ('renamenx to non-existent key', x_get(0, 'q') = 'b');

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'aa', 'bb');
SELECT x_set(0, 'bb', 'cc');

SELECT x_renamenx(0, 'aa', 'bb');

INSERT INTO tests (description, test_result)
VALUES ('renamenx to existent key leaves original', x_get(0, 'aa') = 'bb');

INSERT INTO tests (description, test_result)
VALUES ('renamenx to existent key leaves destination', x_get(0, 'bb') = 'cc');

-------------------
TRUNCATE TABLE kvs;

INSERT INTO tests (description, test_result)
VALUES ('dbsize on empty db', x_dbsize(0) = 0);

SELECT x_set(0, 'a', 'b');
SELECT x_set(0, 'c', 'd');
SELECT x_set(0, 'e', 'f');

INSERT INTO tests (description, test_result)
VALUES ('dbsize on non-empty db', x_dbsize(0) = 3);

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'a', 'b');
SELECT x_set(0, 'c', 'd');

INSERT INTO tests (description, test_result)
VALUES ('mget on multiple existent keys',
        x_mget(0, array['a', 'c']::TEXT[]) = array['b', 'd']::TEXT[]);

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'a', 'b');
SELECT x_set(0, 'c', 'd');

INSERT INTO tests (description, test_result)
VALUES ('mget on mixed existent/non-existent keys',
        x_mget(0, array['a', 'x', 'c']::TEXT[]) =
                  array['b', NULL, 'd']::TEXT[]);

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'a', 'b');
-- Warning, this test will become obselete in August 2169.
SELECT x_expire(0, 'a'::TEXT, TIMESTAMP '2169-08-15 12:34:56');

INSERT INTO tests (description, test_result)
VALUES ('can fetch non-expired result', x_get(0, 'a') = 'b');

-------------------
TRUNCATE TABLE kvs;

SELECT x_set(0, 'a', 'b');
-- Warning, this test will become obselete in August 2169.
SELECT x_expire(0, 'a'::TEXT, TIMESTAMP '2008-08-15 12:34:56');

INSERT INTO tests (description, test_result)
VALUES ('can NOT fetch expired result', x_get(0, 'a') IS NULL);

-- TODO: Tests with different databases

--------------------------------------------------------------------
--------------------------------------------------------------------

SELECT *
FROM (SELECT COUNT(*) AS total FROM tests) AS t1,
     (SELECT COUNT(*) AS passing FROM tests WHERE test_result = True) AS t2;

--------------------------------------------------------------------
--------------------------------------------------------------------

END;