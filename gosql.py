"""Python client to GoSQL"""

import psycopg2


class Client(object):

    def __init__(self, connection=None):
        if connection is None:
            connection = psycopg2.connect("dbname=gosql user=gosql")
        self.connection = connection

    def get(self, key, default=None):
        cursor = self.connection.cursor()
        cursor.execute("SELECT x_get(%s)", (key,))
        (v,) = cursor.fetchone()
        if v is None:
            return default
        return v

    def set(self, key, value):
        cursor = self.connection.cursor()
        cursor.execute("SELECT x_put(%s, %s)", (key, value))
        self.connection.commit()
        cursor.close()

    def lpush(self, key, element):
        cursor = self.connection.cursor()
        cursor.execute("SELECT x_lpush(%s, %s)", (key, element))
        self.connection.commit()
        cursor.close()

    def rpush(self, key, element):
        cursor = self.connection.cursor()
        cursor.execute("SELECT x_rpush(%s, %s)", (key, element))
        self.connection.commit()
        cursor.close()

    def lpop(self, key, default=None):
        cursor = self.connection.cursor()
        cursor.execute("SELECT x_lpop(%s)", (key,))
        (v,) = cursor.fetchone()
        if v is None:
            return default
        return v

    def rpop(self, key, default=None):
        cursor = self.connection.cursor()
        cursor.execute("SELECT x_rpop(%s)", (key,))
        (v,) = cursor.fetchone()
        if v is None:
            return default
        return v

    def incr(self, key):
        pass

    def decr(self, key):
        pass
