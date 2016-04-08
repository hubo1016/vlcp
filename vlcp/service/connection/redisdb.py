'''
Created on 2016/3/8

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.service.connection.tcpserver import TcpServerBase
from vlcp.protocol.redis import Redis
from vlcp.server.module import api
from vlcp.utils.redisclient import RedisClient
from vlcp.event.runnable import RoutineContainer
import pickle
try:
    import cPickle
except ImportError:
    pass
import json
from vlcp.utils.jsonencoder import encode_default, decode_object
import itertools
try:
    from itertools import izip
except:
    def izip(*args, **kwargs):
        return iter(zip(*args, **kwargs))

import random

class RedisWriteConflictException(Exception):
    pass

@defaultconfig
class RedisDB(TcpServerBase):
    '''
    Create redis clients to connect to redis server
    '''
    _default_url = 'tcp://localhost/'
    _default_db = None
    _default_serialize = 'json'
    _default_pickleversion = 'default'
    _default_cpickle = True
    _default_maxretry = 16
    _default_maxspin = 10
    client = True
    def __init__(self, server):
        self._redis_clients = {}
        TcpServerBase.__init__(self, server, Redis)
        if self.serialize == 'pickle' or self.serialize == 'cpickle' or self.serialize == 'cPickle':
            if self.serialize == 'pickle':
                if not self.cpickle:
                    p = pickle
                else:
                    p = cPickle
            else:
                p = cPickle
            if self.pickleversion is None or self.pickleversion == 'default':
                pickleversion = None
            elif self.pickleversion == 'highest':
                pickleversion = p.HIGHEST_PROTOCOL
            else:
                pickleversion = self.pickleversion
            def _encode(obj):
                return p.dumps(obj, pickleversion)
            self._encode = _encode
            def _decode(data):
                if data is None:
                    return None
                elif isinstance(data, Exception):
                    raise data
                else:
                    return p.loads(data)
            self._decode = _decode
        else:
            def _encode(obj):
                return json.dumps(obj, default=encode_default).encode('utf-8')
            self._encode = _encode
            def _decode(data):
                if data is None:
                    return None
                elif isinstance(data, Exception):
                    raise data
                elif not isinstance(data, str) and isinstance(data, bytes):
                    data = data.decode('utf-8')
                return json.loads(data, object_hook=decode_object)
            self._decode = _decode
        self.appendAPI(api(self.getclient),
                       api(self.get, self.apiroutine),
                       api(self.set, self.apiroutine),
                       api(self.delete, self.apiroutine),
                       api(self.mget, self.apiroutine),
                       api(self.mset, self.apiroutine),
                       api(self.update, self.apiroutine),
                       api(self.mupdate, self.apiroutine),
                       api(self.updateall, self.apiroutine),
                       api(self.updateallwithtime, self.apiroutine))
    def _client_class(self, config, protocol, vhost):
        db = getattr(config, 'db', None)
        def _create_client(url, protocol, scheduler = None, key = None, certificate = None, ca_certs = None, bindaddress = None):
            c = RedisClient(url, db, protocol)
            if key:
                c.key = key
            if certificate:
                c.certificate = certificate
            if ca_certs:
                c.ca_certs = ca_certs
            self._redis_clients[vhost] = c
            return c.make_connobj(self.apiroutine)
        return _create_client
    def getclient(self, vhost = ''):
        "Return a tuple of (redisclient, encoder, decoder) for specified vhost"
        return (self._redis_clients.get(vhost), self._encode, self._decode)
    def get(self, key, timeout = None, vhost = ''):
        "Get value from key"
        c = self._redis_clients.get(vhost)
        if c is None:
            raise ValueError('vhost ' + repr(vhost) + ' is not defined')
        if timeout is not None:
            if timeout <= 0:
                for m in c.batch_execute(self.apiroutine, ('MULTI',), 
                                                        ('GET', key),
                                                        ('DEL', key),
                                                        ('EXEC',)):
                    yield m
            else:
                for m in c.batch_execute(self.apiroutine, ('MULTI',),
                                                        ('GET', key),
                                                        ('PEXPIRE', key, int(timeout * 1000)),
                                                        ('EXEC',)):
                    yield m
            r = self.apiroutine.retvalue[3][0]
            if isinstance(r, Exception):
                raise r
            self.apiroutine.retvalue = self._decode(r)
        else:
            for m in c.execute_command(self.apiroutine, 'GET', key):
                yield m
            self.apiroutine.retvalue = self._decode(self.apiroutine.retvalue)
    def set(self, key, value, timeout = None, vhost = ''):
        "Set value to key, with an optional timeout"
        c = self._redis_clients.get(vhost)
        if timeout is None:
            for m in c.execute_command(self.apiroutine, 'SET', key, self._encode(value)):
                yield m
        elif timeout <= 0:
            for m in c.execute_command(self.apiroutine, 'DEL', key):
                yield m
        else:
            for m in c.execute_command(self.apiroutine, 'PSETEX', key, int(timeout * 1000), self._encode(value)):
                yield m
        self.apiroutine.retvalue = None
    def delete(self, key, vhost = ''):
        c = self._redis_clients.get(vhost)
        for m in c.execute_command(self.apiroutine, 'DEL', key):
            yield m
        self.apiroutine.retvalue = None
    def mget(self, keys, vhost = ''):
        "Get multiple values from multiple keys"
        c = self._redis_clients.get(vhost)
        for m in c.execute_command(self.apiroutine, 'MGET', *keys):
            yield m
        self.apiroutine.retvalue = [self._decode(r) for r in self.apiroutine.retvalue]
    def mset(self, kvpairs, timeout = None, vhost = ''):
        "Set multiple values on multiple keys"
        c = self._redis_clients.get(vhost)
        d = kvpairs
        if hasattr(d, 'items'):
            d = d.items()
        if timeout is not None and timeout <= 0:
            for m in c.execute_command(self.apiroutine, 'DEL', *[k for k,_ in d]):
                yield m
        else:
            if timeout is None:
                for m in c.execute_command(self.apiroutine, 'MSET', *list(itertools.chain.from_iterable(d))):
                    yield m
            else:
                # Use a transact
                ptimeout = int(timeout * 1000)
                
                for m in c.batch_execute(self.apiroutine, *((('MULTI',),
                                                             ('MSET',) + tuple(itertools.chain.from_iterable(d))) + \
                                                            tuple(('PEXPIRE', k, ptimeout) for k, _ in d) + \
                                                            (('EXEC',),))
                                                        ):
                    yield m
        self.apiroutine.retvalue = None
    def update(self, key, updater, timeout = None, vhost = ''):
        '''
        Update in-place with a custom function
        
        :param key: key to update
        
        :param updater: func(k,v), should return a new value to update, or return None to delete. The function
                        may be call more than once.
        
        :param timeout: new timeout
        
        :returns: the updated value, or None if deleted
        '''
        c = self._redis_clients.get(vhost)
        for m in c.get_connection(self.apiroutine):
            yield m
        newconn = self.apiroutine.retvalue
        spin = self.maxspin
        with newconn.context(self.apiroutine):
            for i in range(0, self.maxretry):
                for m in newconn.execute_command(self.apiroutine, 'WATCH', key):
                    yield m
                for m in newconn.execute_command(self.apiroutine, 'GET', key):
                    yield m
                v = self._decode(self.apiroutine.retvalue)
                try:
                    v = updater(key, v)
                    if v is not None:
                        ve = self._encode(v)
                except Exception as exc:
                    for m in newconn.execute_command(self.apiroutine, 'UNWATCH'):
                        yield m
                    raise exc
                if timeout is not None and timeout <= 0 or v is None:
                    set_command = ('DEL', key)
                elif timeout is None:
                    set_command = ('SET', key, ve)
                else:
                    set_command = ('PSETEX', key, int(timeout * 1000), ve)
                for m in newconn.batch_execute(self.apiroutine, ('MULTI',),
                                                                set_command,
                                                                ('EXEC',)):
                    yield m
                r = self.apiroutine.retvalue[2]
                if r is not None:
                    # Succeeded
                    self.apiroutine.retvalue = v
                    raise StopIteration
                else:
                    # Watch keys changed, retry
                    if i > spin:
                        # Wait for a random small while
                        for m in self.apiroutine.waitWithTimeout(random.randrange(0, 1<<(i-spin)) * 0.05):
                            yield m
            raise RedisWriteConflictException('Transaction still fails after many retries: key=' + repr(key))
                
    def mupdate(self, keys, updater, timeout = None, vhost = ''):
        "Update multiple keys in-place with a custom function, see update. Either all success, or all fail."
        c = self._redis_clients.get(vhost)
        for m in c.get_connection(self.apiroutine):
            yield m
        newconn = self.apiroutine.retvalue
        spin = self.maxspin
        with newconn.context(self.apiroutine):
            for i in range(0, self.maxretry):
                for m in newconn.execute_command(self.apiroutine, 'WATCH', *keys):
                    yield m
                for m in newconn.execute_command(self.apiroutine, 'MGET', *keys):
                    yield m
                values = self.apiroutine.retvalue
                try:
                    values = [updater(k, self._decode(v)) for k,v in izip(keys,values)]
                    keys_deleted = [k for k,v in izip(keys,values) if v is None]
                    values_encoded = [(k,self._encode(v)) for k,v in izip(keys,values) if v is not None]
                except Exception as exc:
                    for m in newconn.execute_command(self.apiroutine, 'UNWATCH'):
                        yield m
                    raise exc
                if timeout is None:
                    set_commands_list = []
                    if values_encoded:
                        set_commands_list.append(('MSET',) + tuple(itertools.chain.from_iterable(values_encoded)))
                    if keys_deleted:
                        set_commands_list.append(('DEL',) + tuple(keys_deleted))
                    set_commands = tuple(set_commands_list)
                elif timeout <= 0:
                    set_commands = (('DEL',) + tuple(keys),)
                else:
                    ptimeout = int(timeout * 1000)
                    set_commands_list = []
                    if values_encoded:
                        set_commands_list.append(('MSET',) + tuple(itertools.chain.from_iterable(values_encoded)))
                        set_commands_list.extend(('PEXPIRE', k, ptimeout) for k,_ in values_encoded)
                    if keys_deleted:
                        set_commands_list.append(('DEL',) + tuple(keys_deleted))
                    set_commands = tuple(set_commands_list)
                for m in newconn.batch_execute(self.apiroutine, *((('MULTI',),) + \
                                                                set_commands + \
                                                                (('EXEC',),))):
                    yield m
                r = self.apiroutine.retvalue[-1]
                if r is not None:
                    # Succeeded
                    self.apiroutine.retvalue = values
                    raise StopIteration
                else:
                    # Watch keys changed, retry
                    if i > spin:
                        # Wait for a random small while
                        for m in self.apiroutine.waitWithTimeout(random.randrange(0, 1<<(i-spin)) * 0.05):
                            yield m
            raise RedisWriteConflictException('Transaction still fails after many retries: keys=' + repr(keys))
    def updateall(self, keys, updater, timeout = None, vhost = ''):
        "Update multiple keys in-place, with a function updater(keys, values) which returns (updated_keys, updated_values). Either all success or all fail"
        c = self._redis_clients.get(vhost)
        for m in c.get_connection(self.apiroutine):
            yield m
        newconn = self.apiroutine.retvalue
        spin = self.maxspin
        with newconn.context(self.apiroutine):
            for i in range(0, self.maxretry):
                for m in newconn.execute_command(self.apiroutine, 'WATCH', *keys):
                    yield m
                for m in newconn.execute_command(self.apiroutine, 'MGET', *keys):
                    yield m
                values = self.apiroutine.retvalue
                try:
                    new_keys, new_values = updater(keys, [self._decode(v) for v in values])
                    keys_deleted = [k for k,v in izip(new_keys, new_values) if v is None]
                    values_encoded = [(k,self._encode(v)) for k,v in izip(new_keys, new_values) if v is not None]
                except Exception as exc:
                    for m in newconn.execute_command(self.apiroutine, 'UNWATCH'):
                        yield m
                    raise exc
                if timeout is None:
                    set_commands_list = []
                    if values_encoded:
                        set_commands_list.append(('MSET',) + tuple(itertools.chain.from_iterable(values_encoded)))
                    if keys_deleted:
                        set_commands_list.append(('DEL',) + tuple(keys_deleted))
                    set_commands = tuple(set_commands_list)
                elif timeout <= 0:
                    set_commands = (('DEL',) + tuple(new_keys),)
                else:
                    ptimeout = int(timeout * 1000)
                    set_commands_list = []
                    if values_encoded:
                        set_commands_list.append(('MSET',) + tuple(itertools.chain.from_iterable(values_encoded)))
                        set_commands_list.extend(('PEXPIRE', k, ptimeout) for k,_ in values_encoded)
                    if keys_deleted:
                        set_commands_list.append(('DEL',) + tuple(keys_deleted))
                    set_commands = tuple(set_commands_list)
                for m in newconn.batch_execute(self.apiroutine, *((('MULTI',),) + \
                                                                set_commands + \
                                                                (('EXEC',),))):
                    yield m
                r = self.apiroutine.retvalue[-1]
                if r is not None:
                    # Succeeded
                    self.apiroutine.retvalue = values
                    raise StopIteration
                else:
                    # Watch keys changed, retry
                    if i > spin:
                        # Wait for a random small while
                        for m in self.apiroutine.waitWithTimeout(random.randrange(0, 1<<(i-spin)) * 0.05):
                            yield m
            raise RedisWriteConflictException('Transaction still fails after many retries: keys=' + repr(keys))
    def updateallwithtime(self, keys, updater, timeout = None, vhost = ''):
        "Update multiple keys in-place, with a function updater(keys, values, timestamp) which returns (updated_keys, updated_values). Either all success or all fail. Timestamp is a integer standing for current time in microseconds."
        c = self._redis_clients.get(vhost)
        for m in c.get_connection(self.apiroutine):
            yield m
        newconn = self.apiroutine.retvalue
        spin = self.maxspin
        with newconn.context(self.apiroutine):
            for i in range(0, self.maxretry):
                for m in newconn.execute_command(self.apiroutine, 'WATCH', *keys):
                    yield m
                for m in newconn.batch_execute(self.apiroutine, ('MGET',) + tuple(keys),
                                                                ('TIME',)):
                    yield m
                values, time_tuple = self.apiroutine.retvalue
                server_time = int(time_tuple[0]) * 1000000 + int(time_tuple[1])
                try:
                    new_keys, new_values = updater(keys, [self._decode(v) for v in values], server_time)
                    keys_deleted = [k for k,v in izip(new_keys, new_values) if v is None]
                    values_encoded = [(k,self._encode(v)) for k,v in izip(new_keys, new_values) if v is not None]
                except Exception as exc:
                    for m in newconn.execute_command(self.apiroutine, 'UNWATCH'):
                        yield m
                    raise exc
                if timeout is None:
                    set_commands_list = []
                    if values_encoded:
                        set_commands_list.append(('MSET',) + tuple(itertools.chain.from_iterable(values_encoded)))
                    if keys_deleted:
                        set_commands_list.append(('DEL',) + tuple(keys_deleted))
                    set_commands = tuple(set_commands_list)
                elif timeout <= 0:
                    set_commands = (('DEL',) + tuple(new_keys),)
                else:
                    ptimeout = int(timeout * 1000)
                    set_commands_list = []
                    if values_encoded:
                        set_commands_list.append(('MSET',) + tuple(itertools.chain.from_iterable(values_encoded)))
                        set_commands_list.extend(('PEXPIRE', k, ptimeout) for k,_ in values_encoded)
                    if keys_deleted:
                        set_commands_list.append(('DEL',) + tuple(keys_deleted))
                    set_commands = tuple(set_commands_list)
                for m in newconn.batch_execute(self.apiroutine, *((('MULTI',),) + \
                                                                set_commands + \
                                                                (('EXEC',),))):
                    yield m
                r = self.apiroutine.retvalue[-1]
                if r is not None:
                    # Succeeded
                    self.apiroutine.retvalue = values
                    raise StopIteration
                else:
                    # Watch keys changed, retry
                    if i > spin:
                        # Wait for a random small while
                        for m in self.apiroutine.waitWithTimeout(random.randrange(0, 1<<(i-spin)) * 0.05):
                            yield m
            raise RedisWriteConflictException('Transaction still fails after many retries: keys=' + repr(keys))
    