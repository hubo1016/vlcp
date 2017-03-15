'''
Created on 2016/1/8

:author: hubo
'''
from vlcp.config.config import Configurable, config
from vlcp.event.connection import Client
from vlcp.protocol.redis import Redis, RedisConnectionStateEvent, RedisSubscribeMessageEvent,\
    RedisReplyException
from contextlib import contextmanager, closing

def _str(b, encoding = 'ascii'):
    if isinstance(b, str):
        return b
    elif isinstance(b, bytes):
        return b.decode(encoding)
    else:
        return str(b)

def _conn(func):
    def f(self, container, *args, **kwargs):
        for m in self._get_default_connection(container):
            yield m
        for m in func(self, container, *args, **kwargs):
            yield m
    return f

class RedisConnectionDown(IOError):
    pass

class RedisConnectionRestarted(RedisConnectionDown):
    pass

@config('redisclient')
class RedisClientBase(Configurable):
    '''
    Connect to Redis server
    '''
    # Default connection URL for Redis client
    _default_url = 'tcp://localhost/'
    # Wait for the connection setup before raise an exception
    _default_timeout = 10
    # Select database
    _default_db = 0
    def __init__(self, conn = None, parent = None, protocol = None):
        Configurable.__init__(self)
        self._defaultconn = conn
        self._parent = parent
        self._lockconnmark = None
        if protocol:
            self._protocol = protocol
        else:
            if parent:
                self._protocol = parent._protocol
            else:
                self._protocol = Redis()
    def _get_connection(self, container, connection):
        if not connection.connected:
            for m in container.waitWithTimeout(self.timeout, self._protocol.statematcher(connection, RedisConnectionStateEvent.CONNECTION_UP, False)):
                yield m
            if container.timeout:
                raise RedisConnectionDown('Disconnected from redis server')        
    def _get_default_connection(self, container):
        if not self._defaultconn:
            raise RedisConnectionDown('Not connected to redis server')
        if self._lockconnmark is not None:
            if self._lockconnmark >= 0:
                if not self._defaultconn.connected or self._defaultconn.connmark != self._lockconnmark:
                    raise RedisConnectionRestarted('Disconnected from redis server; reconnected is not allowed in with scope')
                else:
                    return
        for m in self._get_connection(container, self._defaultconn):
            yield m
        if self._lockconnmark is not None and self._lockconnmark < 0:
            self._lockconnmark = self._defaultconn.connmark
    def _shutdown_conn(self, container, connection):
        if connection:
            if connection.connected:
                # Send quit
                try:
                    for m in self._protocol.send_command(connection, container, 'QUIT'):
                        yield m
                    for m in container.waitWithTimeout(1, self._protocol.statematcher(connection)):
                        yield m
                except Exception:
                    for m in connection.shutdown():
                        yield m
                else:
                    if container.timeout:
                        for m in connection.shutdown(True):
                            yield m
            else:
                for m in connection.shutdown():
                    yield m
    def shutdown(self, container):
        '''
        Shutdown all connections to Redis server
        '''
        c = self._defaultconn
        self._defaultconn = None
        for m in self._shutdown_conn(container, c):
            yield m
    def release(self, container):
        '''
        Release the connection, leave it to be reused later.
        '''
        if not self._parent:
            for m in self.shutdown(container):
                yield m
        else:
            for m in self._parent._release_conn(container, self._defaultconn):
                yield m
    @contextmanager
    def context(self, container, release = True, lockconn = True):
        '''
        Use with statement to manage the connection
        
        :params release: if True(default), release the connection when leaving with scope
        
        :params lockconn: if True(default), do not allow reconnect during with scope;
                execute commands on a disconnected connection raises Exceptions.
        '''
        try:
            if lockconn:
                if self._lockconnmark is None:
                    # Lock next connmark
                    self._lockconnmark = -1
                    locked = True
            yield self
        finally:
            if locked:
                self._lockconnmark = None
            self._lockconnmark = None
            if release:
                container.subroutine(self.release(container), False)
    @_conn
    def execute_command(self, container, *args):
        '''
        execute command on current connection
        '''
        for m in self._protocol.execute_command(self._defaultconn, container, *args):
            yield m
    @_conn
    def batch_execute(self, container, *cmds):
        '''
        execute a batch of commands on current connection in pipeline mode
        '''
        for m in self._protocol.batch_execute(self._defaultconn, container, *cmds):
            yield m
    def register_script(self, container, script):
        '''
        register a script to this connection.
        
        :returns: registered script. This is a tuple (sha1, script). Pass the tuple to
                  eval_registered, ensure_registerd as registerd_script parameter.
        '''
        if len(script) < 43:
            container.retvalue = (None, script)
        else:
            for m in self.execute_command(container, 'SCRIPT', 'LOAD', script):
                yield m
            container.retvalue = (container.retvalue, script)
    def eval_registered(self, container, registerd_script, *args):
        '''
        eval a registered script. If the script is not cached on the server, it is automatically cached. 
        '''
        if registerd_script[0]:
            try:
                for m in self.execute_command(container, 'EVALSHA', registerd_script[0], *args):
                    yield m
            except RedisReplyException as exc:
                if exc.subtype == 'NOSCRIPT':
                    for m in self.execute_command(container, 'EVAL', registerd_script[1], *args):
                        yield m
                else:
                    raise
        else:
            for m in self.execute_command(container, 'EVAL', registerd_script[1], *args):
                yield m
    def ensure_registerd(self, container, *scripts):
        '''
        Ensure that these scripts are cached on the server. Important when using scripts with batch_execute.
        :param container: routine container.
        :param \*scripts: registered script tuples, return value of register_script 
        '''
        loading = dict((s[0], s[1]) for s in scripts if s[0])
        if loading:
            keys = list(loading.keys())
            for m in self.execute_command(container, 'SCRIPT', 'EXISTS', *keys):
                yield m
            r = container.retvalue
            cmds = [('SCRIPT', 'LOAD', s) for s in (loading[keys[i]] for i in range(0, len(keys)) if not r[i])]
            if cmds:
                for m in self.batch_execute(container, cmds):
                    yield m
    
class RedisClient(RedisClientBase):
    def __init__(self, url = None, db = None, protocol = None):
        '''
        Redis client to communicate with Redis server. Several connections are created for different functions.
         
        :param url: connectiom url, e.g. 'tcp://localhost/'.
                    If not specified, redisclient.url in configuration is used
        
        :param db: default database. If not specified, redisclient.db in configuration is used,
                   which defaults to 0.
        
        :param protocol: use a pre-created protocol instance instead of creating a new instance
        '''
        RedisClientBase.__init__(self, protocol=protocol)
        if url:
            self.url = url
        if db is not None:
            self.db = db
        self._subscribeconn = None
        self._subscribecounter = {}
        self._psubscribecounter = {}
        self._connpool = []
        self._shutdown = False
    def _create_client(self, container):
        if self._shutdown:
            raise IOError('RedisClient already shutdown')
        conn = Client(self.url, self._protocol, container.scheduler,
                                       getattr(self, 'key', None),
                                       getattr(self, 'certificate', None),
                                       getattr(self, 'ca_certs', None))
        conn.start()
        return conn
    def _get_default_connection(self, container):
        if not self._defaultconn:
            self._defaultconn = self._create_client(container)
            for m in RedisClientBase._get_default_connection(self, container):
                yield m
            for m in self._protocol.send_command(self._defaultconn, container, 'SELECT', str(self.db)):
                yield m
        else:
            for m in RedisClientBase._get_default_connection(self, container):
                yield m
    def _get_subscribe_connection(self, container):
        if not self._subscribeconn:
            self._subscribeconn = self._create_client(container)
        for m in RedisClientBase._get_connection(self, container, self._subscribeconn):
            yield m
    def get_connection(self, container):
        '''
        Get an exclusive connection, useful for blocked commands and transactions.
        
        You must call release or shutdown (not recommanded) to return the connection after use.
        
        :param container: routine container
        
        :returns: RedisClientBase object, with some commands same as RedisClient like execute_command,
                  batch_execute, register_script etc.
        '''
        if self._connpool:
            conn = self._connpool.pop()
            container.retvalue = RedisClientBase(conn, self)
        else:
            conn = self._create_client(container)
            for m in RedisClientBase._get_connection(self, container, conn):
                yield m
            for m in self._protocol.send_command(conn, container, 'SELECT', str(self.db)):
                yield m
            container.retvalue = RedisClientBase(conn, self)
    def _release_conn(self, container, connection):
        if connection:
            if self._shutdown or not connection.connected:
                for m in self._shutdown_conn(container, connection):
                    yield m
            else:
                if connection.redis_select != str(self.db):
                    for m in self._protocol.send_command(connection, container, 'SELECT', str(self.db)):
                        yield m
                self._connpool.append(connection)
    def execute_command(self, container, *args):
        '''
        Execute command on Redis server:
          - For (P)SUBSCRIBE/(P)UNSUBSCRIBE, the command is sent to the subscribe connection.
            It is recommended to use (p)subscribe/(p)unsubscribe method instead of directly call the command
          - For BLPOP, BRPOP, BRPOPLPUSH, the command is sent to a separated connection. The connection is
            recycled after command returns.
          - For other commands, the command is sent to the default connection.
        '''
        if args:
            cmd = _str(args[0]).upper()
            if cmd in ('SUBSCRIBE', 'UNSUBSCRIBE', 'PSUBSCRIBE', 'PUNSUBSCRIBE'):
                for m in self._get_subscribe_connection(container):
                    yield m
                for m in self._protocol.execute_command(self._subscribeconn, container, *args):
                    yield m
                return
            elif cmd in ('BLPOP', 'BRPOP', 'BRPOPLPUSH'):
                def _blocked_command():
                    for m in self.get_connection(container):
                        yield m
                    c = container.retvalue
                    with c.context(container):
                        try:
                            for m in c.execute_command(container, *args):
                                yield m
                        except BaseException as exc:
                            for m in c.shutdown(container):
                                yield m
                            raise exc
                with closing(container.delegateOther(_blocked_command(), container, forceclose = True)) as g:
                    for m in g:
                        yield m
                return
        for m in RedisClientBase.execute_command(self, container, *args):
            yield m
    def subscribe(self, container, *keys):
        '''
        Subscribe to specified channels
        :param container: routine container
        :param *keys: subscribed channels
        :returns: list of event matchers for the specified channels
        '''
        for m in self._get_subscribe_connection(container):
            yield m
        realkeys = []
        for k in keys:
            count = self._subscribecounter.get(k, 0)
            if count == 0:
                realkeys.append(k)
            self._subscribecounter[k] = count + 1
        if realkeys:
            for m in self._protocol.execute_command(self._subscribeconn, container, 'SUBSCRIBE', *realkeys):
                yield m
        container.retvalue = [self._protocol.subscribematcher(self._subscribeconn, k, None, RedisSubscribeMessageEvent.MESSAGE) for k in keys]
    def unsubscribe(self, container, *keys):
        '''
        Unsubscribe specified channels. Every subscribed key should be unsubscribed exactly once, even if duplicated subscribed.
        :param container: routine container
        :param *keys: subscribed channels
        '''
        for m in self._get_subscribe_connection(container):
            yield m
        realkeys = []
        for k in keys:
            count = self._subscribecounter.get(k, 0)
            if count <= 1:
                realkeys.append(k)
                try:
                    del self._subscribecounter[k]
                except KeyError:
                    pass
            else:
                self._subscribecounter[k] = count - 1
        if realkeys:
            for m in self._protocol.execute_command(self._subscribeconn, container, 'UNSUBSCRIBE', *realkeys):
                yield m
        container.retvalue = None
    def psubscribe(self, container, *keys):
        '''
        Subscribe to specified globs
        :param container: routine container
        :param *keys: subscribed globs
        :returns: list of event matchers for the specified globs 
        '''
        for m in self._get_subscribe_connection(container):
            yield m
        realkeys = []
        for k in keys:
            count = self._psubscribecounter.get(k, 0)
            if count == 0:
                realkeys.append(k)
            self._psubscribecounter[k] = count + 1
        for m in self._protocol.execute_command(self._subscribeconn, container, 'PSUBSCRIBE', *realkeys):
            yield m
        container.retvalue = [self._protocol.subscribematcher(self._subscribeconn, k, None, RedisSubscribeMessageEvent.PMESSAGE) for k in keys]
    def punsubscribe(self, container, *keys):
        '''
        Unsubscribe specified globs. Every subscribed glob should be unsubscribed exactly once, even if duplicated subscribed.
        
        :param container: routine container
        
        :param *keys: subscribed globs
        '''
        for m in self._get_subscribe_connection(container):
            yield m
        realkeys = []
        for k in keys:
            count = self._subscribecounter.get(k, 0)
            if count == 1:
                realkeys.append(k)
                del self._subscribecounter[k]
            else:
                self._subscribecounter[k] = count - 1
        for m in self._protocol.execute_command(self._subscribeconn, container, 'PUNSUBSCRIBE', *keys):
            yield m
    def shutdown(self, container):
        '''
        Shutdown all connections. Exclusive connections created by get_connection will shutdown after release()
        '''
        p = self._connpool
        self._connpool = []
        self._shutdown = True
        if self._defaultconn:
            p.append(self._defaultconn)
            self._defaultconn = None
        if self._subscribeconn:
            p.append(self._subscribeconn)
            self._subscribeconn = None
        for m in container.executeAll([self._shutdown_conn(container, o)
                                       for o in p]):
            yield  m
    class _RedisConnection(object):
        def __init__(self, client, container):
            self._client = client
            self._container = container
        def start(self, asyncstart = False):
            pass
        def shutdown(self):
            if self._client:
                try:
                    for m in self._container.delegate(self._client.shutdown(self._container), True):
                        yield m
                finally:
                    self._client = None
                    self._container = None
    def make_connobj(self, container):
        '''
        Return an object to be used like a connection. Put the connection-like object in module.connections
        to make RedisClient shutdown on module unloading.
        '''
        return self._RedisConnection(self, container)
    def subscribe_state_matcher(self, container, connected = True):
        '''
        Return a matcher to match the subscribe connection status.
        
        :param container: a routine container. NOTICE: this method is not a routine.
        
        :param connected: if True, the matcher matches connection up. If False, the matcher matches
               connection down.
        
        :returns: an event matcher.
        '''
        if not self._subscribeconn:
            self._subscribeconn = self._create_client(container)
        return RedisConnectionStateEvent.createMatcher(
                    RedisConnectionStateEvent.CONNECTION_UP if connected else RedisConnectionStateEvent.CONNECTION_DOWN,
                    self._subscribeconn
                    )
        