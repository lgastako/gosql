"""Python client to GoSQL"""

import psycopg2


class Client(object):

    def __init__(self, connection=None):
        if connection is None:
            connection = psycopg2.connect("dbname=gosql user=gosql")
        self.connection = connection

    def _do_single_read(self, sql, *args):
        cursor = self.connection.cursor()
        #print "SQL: %s" % cursor.mogrify(sql, args)
        cursor.execute(sql, args)
        results = cursor.fetchone()
        self.connection.rollback()
        return results[0]

    def _do_write(self, sql, *args):
        cursor = self.connection.cursor()
        cursor.execute(sql, args)
        self.connection.commit()
        cursor.close()

    def _default(self, v, default):
        if v is None:
            return default
        return v

    def get(self, key, default=None):
        v = self._do_single_read("SELECT x_get(%s)", key)
        return self._default(v, default)

    def set(self, key, value):
        self._do_write("SELECT x_set(%s, %s)", key, value)

    def delete(self, key):
        self._do_write("SELECT x_del(%s)", key)

    def lpush(self, key, element):
        self._do_write("SELECT x_lpush(%s, %s)", key, element)

    def rpush(self, key, element):
        self.do_write("SELECT x_rpush(%s, %s)", key, element)

    def lpop(self, key, default=None):
        v = self._do_single_read("SELECT x_lpop(%s)", key)
        return self._default(v, default)

    def rpop(self, key, default=None):
        v = self._do_single_read("SELECT x_rpop(%s)", key)
        return self._default(v, default)

    def exists(self, key):
        v = self._do_single_read("SELECT x_exists(%s)", key)
        return bool(v)

    def rename(self, old_key, new_key):
        self._do_write("SELECT x_rename(%s, %s)", old_key, new_key)

    def renamenx(self, old_key, new_key):
        self._do_write("SELECT x_renamenx(%s, %s)", old_key, new_key)

    def dbsize(self):
        v = self._do_single_read("SELECT x_dbsize()")
        return v

    def mget(self, keys):
        v = self._do_single_read("SELECT x_mget(%s)", keys)
        return v

    def expire(self, key, timestamp):
        self._do_write("SELECT x_expire(%s, %s)", key, timestamp)
