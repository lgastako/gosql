BEGIN;

DROP TABLE IF EXISTS kvs;
DROP FUNCTION IF EXISTS expired(ts TIMESTAMP WITHOUT TIME ZONE);
DROP FUNCTION IF EXISTS x_exists(in_k TEXT);
DROP FUNCTION IF EXISTS x_get(in_k TEXT);
DROP FUNCTION IF EXISTS x_set(in_k TEXT, in_v TEXT);
DROP FUNCTION IF EXISTS x_del(in_k TEXT);
DROP FUNCTION IF EXISTS x_lpush(in_k TEXT, in_v TEXT);
DROP FUNCTION IF EXISTS x_rpush(in_k TEXT, in_v TEXT);
DROP FUNCTION IF EXISTS x_lpop(in_k TEXT);
DROP FUNCTION IF EXISTS x_rpop(in_k TEXT);
DROP FUNCTION IF EXISTS x_rename(old_k TEXT, new_k TEXT);
DROP FUNCTION IF EXISTS x_renamenx(old_k TEXT, new_k TEXT);
DROP FUNCTION IF EXISTS x_dbsize();
DROP FUNCTION IF EXISTS x_mget(in_ks TEXT[]);

CREATE TABLE kvs (
    k TEXT PRIMARY KEY,
    v TEXT,
    a TEXT[],
    use_by TIMESTAMP WITHOUT TIME ZONE,
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

CREATE FUNCTION x_exists(in_k TEXT) RETURNS BOOLEAN AS $$
DECLARE
    c INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO c
    FROM kvs
    WHERE k = in_k;
    RETURN c > 0;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_get(in_k TEXT) RETURNS TEXT AS $$
DECLARE
    t_row kvs%ROWTYPE;
BEGIN
    SELECT *
    INTO t_row
    FROM kvs
    WHERE k = in_k
    AND NOT expired(use_by);

    RETURN t_row.v;
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
           use_by = NULL
       WHERE k = in_k;
    ELSE
       INSERT INTO kvs (k, v) VALUES (in_k, in_v);
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_del(in_k TEXT) RETURNS VOID AS $$
BEGIN
    DELETE FROM kvs
    WHERE k = in_k;
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
    SELECT *
    INTO t_row
    FROM kvs
    WHERE k = in_k
    AND NOT expired(use_by);

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
    SELECT *
    INTO t_row
    FROM kvs
    WHERE k = in_k
    AND NOT expired(use_by);

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

CREATE FUNCTION x_rename(old_k TEXT, new_k TEXT) RETURNS VOID AS $$
BEGIN
    DELETE FROM kvs
    WHERE k = new_k;

    UPDATE kvs
    SET k = new_k
    WHERE k = old_k;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_renamenx(old_k TEXT, new_k TEXT) RETURNS BOOLEAN AS $$
DECLARE
    c INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO c
    FROM kvs
    WHERE k = new_k;

    IF c <= 0 THEN
        UPDATE kvs
        SET k = new_k
        WHERE k = old_k;
        RETURN True;
    ELSE
        RETURN False;
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_dbsize() RETURNS INTEGER AS $$
DECLARE
    c INTEGER;
BEGIN
    SELECT COUNT(*) INTO c FROM kvs;
    RETURN c;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION x_mget(in_ks TEXT[]) RETURNS TEXT[] AS $$
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
                 WHERE in_ks @> ARRAY[k]
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

CREATE FUNCTION x_expire(in_k TEXT, in_use_by TIMESTAMP WITHOUT TIME ZONE)
     RETURNS VOID AS $$
BEGIN
    UPDATE kvs
    SET use_by = in_use_by
    WHERE k = in_k;
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

SELECT x_set('a', 'b');

INSERT INTO tests (description, test_result)
VALUES ('existence check for existent key', x_exists('a') = True);

-------------------
TRUNCATE TABLE kvs;

INSERT INTO tests (description, test_result)
VALUES ('existence check for non-existent key', x_exists('abc') = False);

-------------------
TRUNCATE TABLE kvs;

SELECT x_set('a', 'b');

INSERT INTO tests (description, test_result)
VALUES ('getting a valid key', x_get('a') = 'b');

-------------------
TRUNCATE TABLE kvs;

SELECT x_set('a', 'b');
SELECT x_del('a');

INSERT INTO tests (description, test_result)
VALUES ('deleting an existent key', x_get('a') IS NULL);

-------------------
TRUNCATE TABLE kvs;

INSERT INTO tests (description, test_result)
VALUES ('getting an invalid key', x_get('c') IS NULL);

-------------------
TRUNCATE TABLE kvs;

SELECT x_lpush('r', 'foo');
SELECT x_rpush('r', 'bar');

INSERT INTO tests (description, test_result)
VALUES ('lpop once', x_lpop('r') = 'foo');

INSERT INTO tests (description, test_result)
VALUES ('lpop twice', x_lpop('r') = 'bar');

INSERT INTO tests (description, test_result)
VALUES ('lpop thrice', x_lpop('r') IS NULL);

-------------------
TRUNCATE TABLE kvs;

SELECT x_lpush('r', 'foo');
SELECT x_rpush('r', 'bar');

INSERT INTO tests (description, test_result)
VALUES ('rpop once', x_rpop('r') = 'bar');

INSERT INTO tests (description, test_result)
VALUES ('rpop twice', x_rpop('r') = 'foo');

INSERT INTO tests (description, test_result)
VALUES ('rpop thrice', x_rpop('r') IS NULL);

-------------------
TRUNCATE TABLE kvs;

SELECT x_set('a', 'b');
SELECT x_rename('a', 'q');

INSERT INTO tests (description, test_result)
VALUES ('rename to non-existent key', x_get('q') = 'b');

-------------------
TRUNCATE TABLE kvs;

SELECT x_set('aa', 'bb');
SELECT x_set('bb', 'cc');

SELECT x_rename('aa', 'bb');

INSERT INTO tests (description, test_result)
VALUES ('rename to existent key', x_get('bb') = 'bb');

-------------------
TRUNCATE TABLE kvs;

SELECT x_set('a', 'b');
SELECT x_renamenx('a', 'q');

INSERT INTO tests (description, test_result)
VALUES ('renamenx to non-existent key', x_get('q') = 'b');

-------------------
TRUNCATE TABLE kvs;

SELECT x_set('aa', 'bb');
SELECT x_set('bb', 'cc');

SELECT x_renamenx('aa', 'bb');

INSERT INTO tests (description, test_result)
VALUES ('renamenx to existent key leaves original', x_get('aa') = 'bb');

INSERT INTO tests (description, test_result)
VALUES ('renamenx to existent key leaves destination', x_get('bb') = 'cc');

-------------------
TRUNCATE TABLE kvs;

INSERT INTO tests (description, test_result)
VALUES ('dbsize on empty db', x_dbsize() = 0);

SELECT x_set('a', 'b');
SELECT x_set('c', 'd');
SELECT x_set('e', 'f');

INSERT INTO tests (description, test_result)
VALUES ('dbsize on non-empty db', x_dbsize() = 3);

-------------------
TRUNCATE TABLE kvs;

SELECT x_set('a', 'b');
SELECT x_set('c', 'd');

INSERT INTO tests (description, test_result)
VALUES ('mget on multiple existent keys',
        x_mget(array['a', 'c']::TEXT[]) = array['b', 'd']::TEXT[]);

-------------------
TRUNCATE TABLE kvs;

SELECT x_set('a', 'b');
SELECT x_set('c', 'd');

INSERT INTO tests (description, test_result)
VALUES ('mget on mixed existent/non-existent keys',
        x_mget(array['a', 'x', 'c']::TEXT[]) = array['b', NULL, 'd']::TEXT[]);

-------------------
TRUNCATE TABLE kvs;

SELECT x_set('a', 'b');
-- Warning, this test will become obselete in August 2169.
SELECT x_expire('a'::TEXT, TIMESTAMP '2169-08-15 12:34:56');

INSERT INTO tests (description, test_result)
VALUES ('can fetch non-expired result', x_get('a') = 'b');

-------------------
TRUNCATE TABLE kvs;

SELECT x_set('a', 'b');
-- Warning, this test will become obselete in August 2169.
SELECT x_expire('a'::TEXT, TIMESTAMP '2008-08-15 12:34:56');

INSERT INTO tests (description, test_result)
VALUES ('can NOT fetch expired result', x_get('a') IS NULL);



--------------------------------------------------------------------
--------------------------------------------------------------------

SELECT *
FROM (SELECT COUNT(*) AS total FROM tests) AS t1,
     (SELECT COUNT(*) AS passing FROM tests WHERE test_result = True) AS t2;

--------------------------------------------------------------------
--------------------------------------------------------------------

END;