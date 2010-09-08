"""Python client to GoSQL"""

class Client(object):

    def __init__(self, connection):
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

    def incr(self, key):
        pass

    def decr(self, key):
        pass

    def lpush(self, key, element):
        pass

    def rpush(self, key, element):
        pass

