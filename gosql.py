"""Python client to GoSQL"""

import psycopg2


class Client(object):

    def __init__(self, connection=None):
        if connection is None:
            connection = psycopg2.connect("dbname=gosql user=gosql")
        self.connection = connection
        self.db_num = 0

    def _do_single_read(self, sql, *args):
        cursor = self.connection.cursor()
        #print "SQL: %s" % cursor.mogrify(sql, args)
        cursor.execute(sql, (self.db_num,) + args)
        results = cursor.fetchone()
        self.connection.rollback()
        return results[0]

    def _do_write(self, sql, *args):
        cursor = self.connection.cursor()
        cursor.execute(sql, (self.db_num,) + args)
        self.connection.commit()
        cursor.close()

    def _default(self, v, default):
        if v is None:
            return default
        return v

    def select(self, db_num):
        self.db_num = db_num

    def get(self, key, default=None):
        v = self._do_single_read("SELECT x_get(%s, %s)", key)
        return self._default(v, default)

    def set(self, key, value):
        self._do_write("SELECT x_set(%s, %s, %s)", key, value)

    def delete(self, key):
        self._do_write("SELECT x_del(%s, %s)", key)

    def lpush(self, key, element):
        self._do_write("SELECT x_lpush(%s, %s, %s)", key, element)

    def rpush(self, key, element):
        self.do_write("SELECT x_rpush(%s, %s, %s)", key, element)

    def lpop(self, key, default=None):
        v = self._do_single_read("SELECT x_lpop(%s, %s)", key)
        return self._default(v, default)

    def rpop(self, key, default=None):
        v = self._do_single_read("SELECT x_rpop(%s, %s)", key)
        return self._default(v, default)

    def exists(self, key):
        v = self._do_single_read("SELECT x_exists(%s, %s)", key)
        return bool(v)

    def rename(self, old_key, new_key):
        self._do_write("SELECT x_rename(%s, %s, %s)", old_key, new_key)

    def renamenx(self, old_key, new_key):
        self._do_write("SELECT x_renamenx(%s, %s, %s)", old_key, new_key)

    def dbsize(self):
        v = self._do_single_read("SELECT x_dbsize(%s)")
        return v

    def mget(self, keys):
        v = self._do_single_read("SELECT x_mget(%s, %s)", keys)
        return v

    def expire(self, key, timestamp):
        self._do_write("SELECT x_expire(%s, %s, %s)", key, timestamp)
