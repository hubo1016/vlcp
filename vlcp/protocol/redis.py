'''
Created on 2016/1/5

:author: hubo
'''

from vlcp.protocol import Protocol
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices, ConnectionWriteEvent
import logging
import itertools
from vlcp.event.lock import Lock
from contextlib import closing
try:
    import hiredis
    hiredis_available = True
except:
    hiredis_available = False

@withIndices('state', 'connection', 'connmark', 'createby')
class RedisConnectionStateEvent(Event):
    CONNECTION_UP = 'up'
    CONNECTION_DOWN = 'down'
    CONNECTION_NOTCONNECTED = 'notconnected'

@withIndices('connection', 'connmark', 'id', 'iserror', 'createby')
class RedisResponseEvent(Event):
    pass

@withIndices('type', 'subscribe', 'connection', 'connmark', 'createby')
class RedisSubscribeEvent(Event):
    SUBSCRIBE = 'subscribe'
    UNSUBSCRIBE = 'unsubscribe'
    PSUBSCRIBE = 'psubscribe'
    PUNSUBSCRIBE = 'punsubscribe'

@withIndices('type', 'subscribe', 'channel', 'connection', 'connmark', 'createby')
class RedisSubscribeMessageEvent(Event):
    MESSAGE = 'message'
    PMESSAGE = 'pmessage'

class RedisProtocolException(Exception):
    pass

class RedisReplyException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)
        if args:
            self.subtype, _, self.describe = args[0].partition(' ')
        else:
            self.subtype = ''
            self.describe = ''

def _copy(buffer):
    try:
        if isinstance(buffer, memoryview):
            return buffer.tobytes()
        else:
            return buffer[:]
    except:
        return buffer[:]

def _str(b, encoding = 'ascii'):
    if isinstance(b, str):
        return b
    elif isinstance(b, bytes):
        return b.decode(encoding)
    else:
        return str(b)

class RedisParser(object):
    "Python implemented hiredis.Reader()"
    def __init__(self):
        self._buffer = b''
        self._parser = self._parser_gen()
    def feed(self, data):
        self._buffer += data
    def gets(self):
        try:
            return next(self._parser)
        except StopIteration:
            raise RedisProtocolException(*self._exceptionargs)
        except RedisProtocolException as exc:
            self._exceptionargs = exc.args
            raise
    def _parser_gen(self, startpos = 0):
        try:
            _blk_buffer = []
            while True:
                lastfind = startpos
                while True:
                    le = self._buffer.find(b'\r', lastfind, -1)
                    if le < 0:
                        lastfind = len(self._buffer) - 1
                        if lastfind < startpos:
                            lastfind = startpos
                        if startpos > 0:
                            self._buffer = self._buffer[startpos:]
                            lastfind -= startpos
                            startpos = 0
                        yield False
                    else:
                        break
                c = self._buffer[startpos : startpos + 1]
                if c == b'+':
                    r = startpos + 1
                    startpos = le + 2
                    yield self._buffer[r : le]
                elif c == b'-':
                    r = startpos + 1
                    startpos = le + 2
                    yield RedisReplyException(_str(self._buffer[r : le]))
                elif c == b':':
                    r = startpos + 1
                    startpos = le + 2
                    try:
                        yield int(self._buffer[r : le])
                    except ValueError as exc:
                        raise RedisProtocolException(str(exc))
                elif c == b'$':
                    try:
                        blksize = int(self._buffer[startpos + 1 : le])
                    except ValueError as exc:
                        raise RedisProtocolException(str(exc))
                    startpos = le + 2
                    if blksize < 0:
                        yield None
                    else:
                        while len(self._buffer) - startpos < blksize:
                            _blk_buffer.append(self._buffer[startpos:])
                            blksize -= len(self._buffer) - startpos
                            self._buffer = b''
                            startpos = 0
                            yield False
                        _blk_buffer.append(self._buffer[startpos:blksize+startpos])
                        startpos += blksize
                        while len(self._buffer) - startpos < 2:
                            yield False
                        startpos += 2
                        r = b''.join(_blk_buffer)
                        del _blk_buffer[:]
                        yield r
                elif c == b'*':
                    try:
                        arraysize = int(self._buffer[startpos + 1 : le])
                    except ValueError as exc:
                        raise RedisProtocolException(str(exc))
                    startpos = le + 2
                    if arraysize < 0:
                        yield None
                    else:
                        array = []
                        if arraysize:
                            p = self._parser_gen(startpos)
                            try:
                                while arraysize:
                                    nv = next(p)
                                    if nv is not False:
                                        array.append(nv)
                                        arraysize -= 1
                                    else:
                                        yield False
                            finally:
                                p.close()
                                startpos = self._lastpos
                        yield array
                else:
                    raise RedisProtocolException(repr(c) + ' is not a valid RESP type')
        finally:
            self._lastpos = startpos

@defaultconfig
class Redis(Protocol):
    '''
    Redis (RESP) Protocol
    '''
    _default_persist = True
    # Default Redis server port
    _default_defaultport = 6379
    _default_createqueue = True
    # Limit response messages queue size
    _default_messagequeuesize = 4096
    # Default limit Redis bulk string to 64MB
    _default_bulklimit = 67108864
    # Default limit Redis array level to 128 levels
    _default_levellimit = 128
    # Default encoding when using Unicode strings in Redis commands
    _default_encoding = 'utf-8'
    # Use hiredis if possible
    _default_hiredis = True
    # Send PING command when the connection is idle
    _default_keepalivetime = 10
    # Disconnect when PING command does not have response
    _default_keepalivetimeout = 5
    # Connect timeout
    _default_connect_timeout = 5
    _default_tcp_nodelay = True
    _logger = logging.getLogger(__name__ + '.Redis')
    def __init__(self):
        '''
        Constructor
        '''
        Protocol.__init__(self)
        self.usehiredis = hiredis_available and self.hiredis
        self._format_request_init(self.encoding)
    def init(self, connection):
        for m in Protocol.init(self, connection):
            yield m
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority - 1, RedisConnectionStateEvent.createMatcher(connection = connection), ('connstate', connection)))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority + 1, RedisResponseEvent.createMatcher(connection = connection), ('response', connection), self.messagequeuesize))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority + 1, RedisSubscribeEvent.createMatcher(connection = connection), ('subscribe', connection), self.messagequeuesize))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, RedisSubscribeMessageEvent.createMatcher(connection = connection), ('message', connection), self.messagequeuesize))
        connection.redis_subscribe = False
        connection.redis_select = None
        connection.redis_subscribe_keys = set()
        connection.redis_subscribe_pkeys = set()
        for m in self.reconnect_init(connection):
            yield m
    def _format_request_init(self, encoding):
        def _bytes(a):
            if isinstance(a, bytes):
                return a
            else:
                return str(a).encode(encoding)
        def format_request(*args):
            return b'*' + str(len(args)).encode('ascii') + b'\r\n' + b''.join(itertools.chain.from_iterable((b'$',str(len(sa)).encode('ascii'),b'\r\n',sa,b'\r\n') for sa in (_bytes(a) for a in args)))
        self._bytes = _bytes
        self.format_request = format_request
    def reconnect_init(self, connection):
        connection.xid = 1
        connection.redis_replyxid = 1
        connection.redis_ping = -1
        connection.redis_pingreply = -1
        connection.redis_bufferedxid = 0
        connection.redis_sendbuffer = []
        connection.redis_sender = False
        connection.redis_locker = object()
        write_buffer = []
        if self.usehiredis:
            connection.redis_reader = hiredis.Reader(protocolError = RedisProtocolException, replyError = RedisReplyException)
        else:
            connection.redis_reader = RedisParser()
        if connection.redis_select:
            write_buffer.append(self.format_request(b'SELECT', connection.redis_select))
            connection.xid += 1
        if connection.redis_subscribe:
            if connection.redis_subscribe_keys:
                write_buffer.append(self.format_request(b'SUBSCRIBE', *tuple(connection.redis_subscribe_keys)))
            if connection.redis_subscribe_pkeys:
                write_buffer.append(self.format_request(b'PSUBSCRIBE', *tuple(connection.redis_subscribe_pkeys)))
        connection.scheduler.emergesend(ConnectionWriteEvent(connection, connection.connmark, data=b''.join(write_buffer)))
        for m in connection.waitForSend(RedisConnectionStateEvent(RedisConnectionStateEvent.CONNECTION_UP, connection, connection.connmark, self)):
            yield m
    def closed(self, connection):
        for m in Protocol.closed(self, connection):
            yield m
        self._logger.info('Redis connection is closed on %r', connection)
        for m in connection.waitForSend(RedisConnectionStateEvent(RedisConnectionStateEvent.CONNECTION_DOWN, connection, connection.connmark, self)):
            yield m
    def error(self, connection):
        for m in Protocol.error(self, connection):
            yield m
        self._logger.warning('Redis connection is reset on %r', connection)
        for m in connection.waitForSend(RedisConnectionStateEvent(RedisConnectionStateEvent.CONNECTION_DOWN, connection, connection.connmark, self)):
            yield m
    def replymatcher(self, requestid, connection, iserror = None):
        """
        Create an event matcher to match 
        """
        matcherparam = [connection, connection.connmark, requestid]
        if iserror is not None:
            matcherparam.append(iserror)
        return RedisResponseEvent.createMatcher(*matcherparam)
    def subscribematcher(self, connection, subscribe = None, channel = None, type = None):
        return RedisSubscribeMessageEvent.createMatcher(type, subscribe, channel, connection)
    def statematcher(self, connection, state = RedisConnectionStateEvent.CONNECTION_DOWN, currentconn = True):
        if currentconn:
            return RedisConnectionStateEvent.createMatcher(state, connection, connection.connmark)
        else:
            return RedisConnectionStateEvent.createMatcher(state, connection)

    def _prepare_command(self, connection, args):
        cmdname = _str(args[0]).upper()
        if cmdname in ('SUBSCRIBE', 'UNSUBSCRIBE', 'PSUBSCRIBE', 'PUNSUBSCRIBE'):
            if len(args) <= 1:
                raise RedisProtocolException('(P)SUBSCRIBE/(P)UNSUBSCRIBE must have at least one key as parameter')
            connection.redis_subscribe = True
            if cmdname == 'SUBSCRIBE':
                connection.redis_subscribe_keys.update(self._bytes(a) for a in args[1:])
            elif cmdname == 'UNSUBSCRIBE':
                connection.redis_subscribe_keys.difference_update(self._bytes(a) for a in args[1:])
            elif cmdname == 'PSUBSCRIBE':
                connection.redis_subscribe_pkeys.update(self._bytes(a) for a in args[1:])
            elif cmdname == 'PUNSUBSCRIBE':
                connection.redis_subscribe_pkeys.difference_update(self._bytes(a) for a in args[1:])
            reply_matcher = RedisSubscribeEvent.createMatcher(cmdname.lower(), args[-1], connection, connection.connmark)
        elif connection.redis_subscribe_keys or connection.redis_subscribe_pkeys:
            if cmdname == 'PING':
                rid = connection.redis_ping
                connection.redis_ping -= 1
                reply_matcher = RedisResponseEvent.createMatcher(connection, connection.connmark, rid)
            elif cmdname == 'QUIT':
                rid = 0
                reply_matcher = RedisResponseEvent.createMatcher(connection, connection.connmark, rid)
                connection.need_reconnect = False
            else:
                raise RedisProtocolException('Can only use PING/QUIT/(P)SUBSCRIBE/(P)UNSUBSCRIBE on a subscribed connection')
        else:
            if cmdname == 'SELECT':
                if len(args) != 2:
                    raise RedisProtocolException("wrong number of arguments for 'select' command")
                connection.redis_select = _str(args[1])
            elif cmdname == 'QUIT':
                connection.need_reconnect = False
            rid = connection.xid
            connection.xid += 1
            reply_matcher = RedisResponseEvent.createMatcher(connection, connection.connmark, rid)
        r = self.format_request(*args)
        return r, reply_matcher
    def send_command(self, connection, container, *args):
        '''
        Send command to Redis server.
        
        :param connection: Redis connection
        
        :param container: routine container
        
        :param \*args: command paramters, begin with command name, e.g. `'SET'`,`'key'`,`'value'`
        
        :returns: Event matcher to wait for reply. The value is returned from container.retvalue
        '''
        with closing(container.delegateOther(self._send_command(connection, container, *args),
                                             container, forceclose = True)) as g:
            for m in g:
                yield m
    def _send_command(self, connection, container, *args):
        if not args:
            raise RedisProtocolException('No command name')
        l = Lock(connection.redis_locker, connection.scheduler)
        # The socket write sequence must be the same as the send sequence, add a lock to ensure that
        for m in l.lock(container):
            yield m
        with l:
            r, reply_matcher = self._prepare_command(connection, args)
            for m in connection.write(ConnectionWriteEvent(connection, connection.connmark, data = r), False):
                yield m
        container.retvalue = reply_matcher
    def send_batch(self, connection, container, *cmds):
        '''
        Send multiple commands to redis server at once
        
        :param connection: redis connection
        
        :param container: routine container
        
        :param \*cmds: commands to send. Each command is a tuple/list of bytes/str.
        
        :returns: list of reply event matchers (from container.retvalue)
        '''
        with closing(container.delegateOther(self._send_batch(connection, container, *cmds),
                                         container, forceclose = True)) as g:
            for m in g:
                yield m
    def _send_batch(self, connection, container, *cmds):
        "Use delegate to ensure it always ends"
        if not cmds:
            raise RedisProtocolException('No commands')
        l = Lock(connection.redis_locker, connection.scheduler)
        for m in l.lock(container):
            yield m
        with l:
            commands = []
            matchers = []
            for c in cmds:
                try:
                    r, reply_matcher = self._prepare_command(connection, c)
                    commands.append(r)
                    matchers.append(reply_matcher)
                except:
                    self._logger.warning('Error in one of the commands in a batch: %r. The command is ignored.', c, exc_info = True)
            if not commands:
                raise RedisProtocolException('Error for every command in a batch')
            for m in connection.write(ConnectionWriteEvent(connection, connection.connmark, data = b''.join(commands)), False):
                yield m
        container.retvalue = matchers
    def execute_command(self, connection, container, *args):
        '''
        Send command to Redis server and wait for response
        
        :param connection: Redis connection
        
        :param container: routine container
        
        :param \*args: command paramters, begin with command name, e.g. `'SET'`,`'key'`,`'value'`
        
        :returns: Response from Redis server. The value is returned from container.retvalue
        
        :raises RedisReplyException: Redis server returns an error (e.g. "-ERR ...")
        '''
        for m in self.send_command(connection, container, *args):
            yield m
        rm = container.retvalue
        sm = self.statematcher(connection)
        yield (sm, rm)
        if container.matcher is sm:
            raise RedisProtocolException('Redis connection down before response received')
        else:
            r = container.event.result
            if isinstance(r, Exception):
                raise r
            else:
                container.retvalue = r
    def batch_execute(self, connection, container, *cmds):
        '''
        Send multiple commands to redis server at once, and get responses
        
        :param connection: redis connection
        
        :param container: routine container
        
        :param \*cmds: commands to send. Each command is a tuple/list of bytes/str.
        
        :returns: list of replies (from container.retvalue). Exceptions are NOT raised.
        '''
        if not cmds:
            container.retvalue = []
            return
        for m in self.send_batch(connection, container, *cmds):
            yield m
        matchers = container.retvalue
        sm = self.statematcher(connection)
        retvalue = []
        for m in matchers:
            yield (m, sm)
            if container.matcher is sm:
                raise RedisProtocolException('Redis connection down before response received')
            retvalue.append(container.event.result)
        container.retvalue = retvalue
    def parse(self, connection, data, laststart):
        events = []
        connection.redis_reader.feed(_copy(data))
        while True:
            r = connection.redis_reader.gets()
            if r is False:
                break
            if connection.redis_replyxid < connection.xid:
                events.append(RedisResponseEvent(connection, connection.connmark, connection.redis_replyxid, isinstance(r, Exception), self, result = r))
                connection.redis_replyxid += 1
            elif connection.redis_subscribe:
                if isinstance(r, bytes) or isinstance(r, Exception):
                    events.append(RedisResponseEvent(connection, connection.connmark, 0, isinstance(r, Exception), self, result = r))
                elif r[0] == b'message':
                    events.append(RedisSubscribeMessageEvent(RedisSubscribeMessageEvent.MESSAGE, r[1], r[1],
                                                             connection, connection.connmark, self, message = r[2]))
                elif r[0] == b'pmessage':
                    events.append(RedisSubscribeMessageEvent(RedisSubscribeMessageEvent.PMESSAGE, r[1], r[2],
                                                             connection, connection.connmark, self, message = r[3]))
                elif r[0] == b'pong':
                    events.append(RedisResponseEvent(connection, connection.connmark, connection.redis_pingreply, False, self, result = r[1]))
                    connection.redis_pingreply -= 1
                else:
                    events.append(RedisSubscribeEvent(_str(r[0]), r[1], connection, connection.connmark, self, result = r))
                    if not r[2]:
                        connection.redis_subscribe = False
                        connection.xid += connection.redis_bufferedxid
        if laststart == len(data):
            # Remote write close
            events.append(ConnectionWriteEvent(connection, connection.connmark, data = b'', EOF = True))
        return (events, 0)
    def notconnected(self, connection):
        for m in Protocol.notconnected(self, connection):
            yield m
        for m in connection.waitForSend(RedisConnectionStateEvent(RedisConnectionStateEvent.CONNECTION_NOTCONNECTED,
                                                                  connection, connection.connmark, self)):
            yield m
    def keepalive(self, connection):
        try:
            if connection.redis_replyxid == connection.xid:
                for m in connection.executeWithTimeout(self.keepalivetimeout, self.execute_command(connection, connection, 'PING')):
                    yield m
                if connection.timeout:
                    for m in connection.reset(True):
                        yield m
        except Exception:
            for m in connection.reset(True):
                yield m
    @staticmethod
    def reconnect_timeseq():
        yield 0
        nextSeq = 0.5
        while True:
            yield nextSeq                
            if nextSeq < 16:
                nextSeq = nextSeq * 2
            else:
                nextSeq = 20
        