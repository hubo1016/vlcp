'''
Created on 2016/10/20

:author: hubo
'''
from __future__ import print_function

import vlcp.utils.jsonencoder

def decode_object(obj):
    return obj

vlcp.utils.jsonencoder.decode_object = decode_object

from vlcp.scripts.script import ScriptModule
from vlcp.server.module import findModule, callAPI

class MigrateDB(ScriptModule):
    '''
    Migrate data from one database to another
    
    migratedb.py -f <configfile> [--clean] src_module[:src_vhost] dst_module[:dst_vhost]
    
    src_module, dst_module is one of: redisdb, zookeeperdb, defaultdb
    '''
    _default_knownmodules = {'redisdb': 'vlcp.service.connection.redisdb.RedisDB',
                             'zookeeperdb': 'vlcp.service.connection.zookeeperdb.ZooKeeperDB',
                             'defaultdb': 'vlcp.service.kvdb.storage.KVStorage'}
    options = (('clean', None, False),)
    def run(self, src, dst, clean = None):
        if clean is not None:
            clean = True
        else:
            clean = False
        src_module, sep, src_vhost = src.partition(':')
        dst_module, sep, dst_vhost = dst.partition(':')
        load_modules = [self.knownmodules.get(m, m) for m in set((src_module, dst_module))]
        for m in self.apiroutine.executeAll([self.server.moduleloader.loadByPath(m) for m in load_modules], self.server.moduleloader,
                                            ()):
            yield m
        src_service = findModule(self.knownmodules.get(src_module, src_module))[0]._instance.getServiceName()
        dst_service = findModule(self.knownmodules.get(dst_module, dst_module))[0]._instance.getServiceName()
        if clean:
            for m in self.apiroutine.executeAll([callAPI(self.apiroutine, src_service, 'listallkeys', {'vhost': src_vhost}),
                                                 callAPI(self.apiroutine, dst_service, 'listallkeys', {'vhost': dst_vhost})]):
                yield m
            (src_keys,), (dst_keys,) = self.apiroutine.retvalue
        else:
            for m in callAPI(src_service, 'listallkeys', {'vhost': src_vhost}):
                yield m
            src_keys = self.apiroutine.retvalue
            dst_keys = []
        delete_keys = set(dst_keys)
        delete_keys.difference_update(src_keys)
        print('Migrating from %s (vhost = %r) to %s (vhost = %r)' % (src_service, src_vhost, dst_service, dst_vhost))
        print('Moving %d keys, deleting %d keys' % (len(src_keys), len(delete_keys)))
        print('Please check. Migrating starts in 5 seconds, press Ctrl+C to cancel...')
        for m in self.apiroutine.waitWithTimeout(5):
            yield m
        for i in range(0, len(src_keys), 100):
            move_keys = tuple(src_keys[i:i+100])
            for m in callAPI(self.apiroutine, src_service, 'mget', {'keys': move_keys,
                                                                    'vhost': src_vhost}):
                yield m
            values = self.apiroutine.retvalue
            for m in callAPI(self.apiroutine, dst_service, 'mset', {'kvpairs': tuple(zip(move_keys, values)),
                                                                    'vhost': dst_vhost}):
                yield m
            print('%d keys moved' % (i,))
        print('Deleting old keys...')
        if delete_keys:
            for k in delete_keys:
                for m in callAPI(self.apiroutine, dst_service, 'delete', {'key': k,
                                                                          'vhost': dst_vhost}):
                    yield m
        print('All jobs done.')

if __name__ == '__main__':
    MigrateDB.main()
