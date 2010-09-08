from gosql import Client as GoSQLClient

from redis.client import Redis as RedisClient

gosql = GoSQLClient()
redis = RedisClient()

#%timeit gosql.set("aa", "bb")
