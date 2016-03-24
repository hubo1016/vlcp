'''
Created on 2016/3/22

:author: hubo
'''

import vlcp.service.connection.redisdb as redisdb
from vlcp.server.module import proxy

KVStorage = proxy('KVStorage', redisdb.RedisDB)
