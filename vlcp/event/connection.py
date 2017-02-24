'''
Created on 2015/6/19

:author: hubo
'''
from __future__ import print_function, absolute_import, division 
from .runnable import RoutineContainer, RoutineException
from .core import PollEvent, POLLING_ERR, POLLING_IN, POLLING_HUP
from .event import Event, withIndices
from vlcp.utils import ContextAdapter
from ctypes import create_string_buffer, c_char, Array as _Array, memmove as _memmove
import socket
import errno
import sys
import ssl
import multiprocessing
import threading
import logging
import itertools
import signal
from vlcp.event.core import POLLING_OUT
import traceback
from contextlib import closing
import os

if sys.version_info[0] >= 3:
    from urllib.parse import urlsplit
    from queue import Full, Queue, Empty
else:
    from urlparse import urlsplit
    from Queue import Full, Queue, Empty

try:
    _memoryview = memoryview
except:
    class memoryview(object):              # Fake a memoryview interface
        def __init__(self, buf):
            self._buffer = buf
        def __getitem__(self, s):
            if isinstance(s, slice) and isinstance(self._buffer, _Array):
                start, stop, step = s.indices(len(self._buffer))
                if step != 1:
                    raise ValueError('memoryview does not support slicing with step')
                return (c_char * (stop - start)).from_buffer(self._buffer, start)
            else:
                return self._buffer[s]
        def __setitem__(self, s, value):
            if isinstance(s, slice) and isinstance(self._buffer, _Array):
                start, stop, step = s.indices(len(self._buffer))
                if step != 1:
                    raise ValueError('memoryview does not support slicing with step')
                if len(value) != stop - start:
                    raise ValueError('assignment length not match')
                _memmove((c_char * (stop - start)).from_buffer(self._buffer, start), value, stop - start)
            else:
                self._buffer[s] = value
        def __len__(self):
            return len(self._buffer)
        def tobytes(self):
            return self._buffer[:]

if sys.version_info[0] >= 3:
    _create_buffer = bytearray
    def _extend_buffer(source, size):
        _new_buffer = _create_buffer(size)
        _new_buffer[0:len(source)] = source
        return _new_buffer
    def _buffer(data, view, start, length):
        return view[start:start+length]
else:
    _create_buffer = create_string_buffer
    def _extend_buffer(source, size):
        return create_string_buffer(source.raw, size)
    # re does not accept memoryview
    def _buffer(data, view, start, length):
        return buffer(data, start, length)

@withIndices('connection', 'type', 'force', 'connmark')
class ConnectionControlEvent(Event):
    SHUTDOWN = 'shutdown'
    RECONNECT = 'reconnect'
    RESET = 'reset'
    STOPLISTEN = 'stoplisten'
    STARTLISTEN = 'startlisten'
    HANGUP = 'hangup'

@withIndices('connection', 'connmark')
class ConnectionWriteEvent(Event):
    """
    Event used to send data to a connection
    """
    canignore = False
    def canignorenow(self):
        return not self.connection.connected or self.connection.connmark != self.connmark

class ConnectionResetException(Exception):
    pass

class Connection(RoutineContainer):
    '''
    A connection on a socket
    '''
    logger = logging.getLogger(__name__ +'.Connection')
    def __init__(self, protocol, sockobj = None, scheduler = None):
        '''
        Constructor
        '''
        RoutineContainer.__init__(self, scheduler)
        self.connected = False
        self.protocol = protocol
        self.socket = sockobj
        if self.socket is not None:
            self.socket.setblocking(False)
        self.persist = getattr(protocol, 'persist', False)
        self.need_reconnect = self.persist
        self.keepalivetime = getattr(protocol, 'keepalivetime', None)
        self.logContext = {'connection': self, 'protocol':protocol, 'socket':None if self.socket is None else self.socket.fileno()}
        self.logger = ContextAdapter(Connection.logger, {'context':self.logContext}) 
        # Counters
        self.totalrecv = 0
        self.totalsend = 0
        self.connrecv = 0
        self.connsend = 0
        self.daemon = False
    def attach(self, sockobj):
        self.socket = sockobj
        self.logContext['socket'] = None if self.socket is None else self.socket.fileno()
        self.logger = ContextAdapter(Connection.logger, {'context': self.logContext})
        if self.socket is not None:
            self.socket.setblocking(False)
    def setdaemon(self, daemon):
        if self.daemon != daemon:
            if self.socket:
                self.scheduler.setPollingDaemon(self.socket, daemon)
            if hasattr(self, 'mainroutine'):
                self.scheduler.setDaemon(self.mainroutine, daemon, True)
            self.daemon = daemon
    def _read_main(self):
        try:
            canread_matcher = PollEvent.createMatcher(self.socket.fileno(), PollEvent.READ_READY)
            canwrite_matcher = PollEvent.createMatcher(self.socket.fileno(), PollEvent.WRITE_READY)
            self.readstop = False
            keepalivetime = self.keepalivetime
            buffersize = getattr(self.protocol, 'buffersize', 4096)
            buf = _create_buffer(buffersize)
            view = memoryview(buf)
            currPos = 0
            lastPos = 0
            exitLoop = False
            connection_control = ConnectionControlEvent.createMatcher(self, None, False, _ismatch = lambda x: x.connmark == self.connmark or x.connmark < 0)
            firstTime = True
            wantWrite = False
            eofExit = False
            while not exitLoop:
                if not firstTime:
                    if wantWrite:
                        yield (canwrite_matcher, connection_control)
                    else:
                        for m in self.waitWithTimeout(keepalivetime, canread_matcher, connection_control):
                            yield m
                        if self.timeout:
                            self.subroutine(self.protocol.keepalive(self))
                            continue
                    wantWrite = False
                    if self.matcher is connection_control:
                        if self.event.type == ConnectionControlEvent.RECONNECT:
                            self.need_reconnect = True
                        elif self.event.type == ConnectionControlEvent.SHUTDOWN:
                            self.need_reconnect = False
                        else:
                            self.need_reconnect = self.persist
                        exitLoop = True
                else:
                    firstTime = False
                parsed = True
                while True:
                    try:
                        currLen = self.socket.recv_into(view[currPos:])
                        if currLen == 0:
                            eofExit = True
                            exitLoop = True
                            break
                        else:
                            parsed = False
                    except ssl.SSLError as exc_ssl:
                        if exc_ssl.args[0] == ssl.SSL_ERROR_WANT_READ:
                            break
                        elif exc_ssl.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                            wantWrite = True
                            break
                        else:
                            exitLoop = True
                            break
                    except socket.error as exc:
                        if exc.errno == errno.EAGAIN or exc.errno == errno.EWOULDBLOCK:
                            break
                        elif exc.errno == errno.EINTR:
                            continue
                        else:
                            exitLoop = True
                            break
                    self.totalrecv += currLen
                    self.connrecv += currLen
                    currPos = currLen + currPos
                    if currPos >= len(buf):
                        try:
                            (events, keep) = self.protocol.parse(self, _buffer(buf, view, 0, currPos), lastPos)
                        except:
                            # An exception in parse means serious protocol break, drop the connection
                            # Shutdown will prevent further data been sent in the socket and break the connection
                            self.socket.shutdown(socket.SHUT_RDWR)
                            raise
                        view[0:keep] = view[currPos-keep:currPos]
                        newPos = keep
                        parsed = True
                        for e in events:
                            for m in self.waitForSend(e):
                                while True:
                                    yield m + (connection_control,)
                                    if self.matcher is connection_control:
                                        if self.event.type == ConnectionControlEvent.RECONNECT:
                                            self.need_reconnect = True
                                        elif self.event.type == ConnectionControlEvent.SHUTDOWN:
                                            self.need_reconnect = False
                                        else:
                                            self.need_reconnect = self.persist
                                        exitLoop = True
                                    else:
                                        break
                        currPos = newPos
                        lastPos = newPos
                        if currPos >= len(buf):
                            buffer2 = _extend_buffer(buf, len(buf) * 2)
                            buf = buffer2
                            view = memoryview(buf)
                        for m in self.doEvents():
                            while True:
                                yield m + (connection_control,)
                                if self.matcher is connection_control:
                                    if self.event.type == ConnectionControlEvent.RECONNECT:
                                        self.need_reconnect = True
                                    elif self.event.type == ConnectionControlEvent.SHUTDOWN:
                                        self.need_reconnect = False
                                    else:
                                        self.need_reconnect = self.persist
                                    exitLoop = True
                                else:
                                    break
                if not parsed:
                    (events, keep) = self.protocol.parse(self, _buffer(buf, view, 0, currPos), lastPos)
                    view[0:keep] = view[currPos-keep:currPos]
                    newPos = keep
                    currPos = newPos
                    lastPos = newPos
                    parsed = True
                    for e in events:
                        for m in self.waitForSend(e):
                            yield m
                if eofExit:
                    # An extra parse for socket shutdown, can be determined by lastPos == len(data)
                    (events, keep) = self.protocol.parse(self, _buffer(buf, view, 0, currPos), lastPos)
                    for e in events:
                        for m in self.waitForSend(e):
                            yield m                    
        finally:
            self.readstop = True
            if self.writestop and self.connected:
                self.scheduler.emergesend(ConnectionControlEvent(self, ConnectionControlEvent.HANGUP, True, self.connmark))
    def _write_main(self):
        try:
            self.writestop = False
            canread_matcher = PollEvent.createMatcher(self.socket.fileno(), PollEvent.READ_READY)
            canwrite_matcher = PollEvent.createMatcher(self.socket.fileno(), PollEvent.WRITE_READY)
            connection_control = ConnectionControlEvent.createMatcher(self, None, False, _ismatch = lambda x: x.connmark == self.connmark or x.connmark < 0)
            write_matcher = ConnectionWriteEvent.createMatcher(self, self.connmark)
            queue_empty = None
            exitLoop = False
            while not exitLoop:
                if queue_empty is not None:
                    yield (write_matcher, queue_empty)
                else:
                    yield (write_matcher, connection_control)
                if self.matcher is connection_control:
                    if self.event.type == ConnectionControlEvent.RECONNECT:
                        self.need_reconnect = True
                    elif self.event.type == ConnectionControlEvent.SHUTDOWN:
                        self.need_reconnect = False
                    else:
                        self.need_reconnect = self.persist
                    if hasattr(self, 'queue'):
                        queue_empty = self.queue.waitForEmpty()
                        if queue_empty is None:
                            exitLoop = True
                    else:
                        exitLoop = True
                elif self.matcher is queue_empty:
                    queue_empty = self.queue.waitForEmpty()
                    if queue_empty is None:
                        exitLoop = True
                else:
                    self.event.canignore = True
                    msg, isEOF = self.protocol.serialize(self, self.event)
                    msg = memoryview(msg)
                    totalLen = len(msg)
                    currPos = 0
                    while currPos < totalLen:
                        wouldblock = False
                        wantRead = False
                        try:
                            currLen = self.socket.send(msg[currPos:])
                            currPos += currLen
                            self.totalsend += currLen
                            self.connsend += currLen
                        except ssl.SSLError as exc_ssl:
                            if exc_ssl.args[0] == ssl.SSL_ERROR_WANT_READ:
                                wantRead = True
                                wouldblock = True
                            elif exc_ssl.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                                wantRead = False
                                wouldblock = True
                            else:
                                raise
                        except socket.error as exc:
                            if exc.errno == errno.EAGAIN or exc.errno == errno.EWOULDBLOCK:
                                wouldblock = True
                            elif exc.errno == errno.EPIPE or exc.errno == errno.ECONNRESET or exc.errno == errno.ECONNABORTED:
                                exitLoop = True
                                break
                            elif exc.errno == errno.EINTR:
                                continue
                            else:
                                raise
                        if wouldblock:
                            if wantRead:
                                yield (canread_matcher, connection_control)
                            else:
                                yield (canwrite_matcher, connection_control)
                            if self.matcher is connection_control:
                                if self.event.type == ConnectionControlEvent.RECONNECT:
                                    self.need_reconnect = True
                                elif self.event.type == ConnectionControlEvent.SHUTDOWN:
                                    self.need_reconnect = False
                                else:
                                    self.need_reconnect = self.persist
                                if hasattr(self, 'queue'):
                                    queue_empty = self.queue.waitForEmpty()
                                    if queue_empty is None:
                                        exitLoop = True
                                else:
                                    exitLoop = True
                    if isEOF:
                        try:
                            self.socket.shutdown(socket.SHUT_WR)
                        except socket.error:
                            # The socket may already be closed or reset
                            pass
                        exitLoop = True
        finally:
            self.writestop = True
            if self.readstop and self.connected:
                self.scheduler.emergesend(ConnectionControlEvent(self, ConnectionControlEvent.HANGUP, True, self.connmark))
    def _reconnect(self):
        self.connected = False
        if False:
            yield
    def _close(self):
        if hasattr(self, 'readroutine'):
            self.readroutine.close()
        if hasattr(self, 'writeroutine'):
            self.writeroutine.close()
        if self.socket is not None:
            self.scheduler.unregisterPolling(self.socket, self.daemon)
            self.socket.close()
            self.socket = None
        self.connected = False
    def main(self):
        try:
            try:
                self.localaddr = self.socket.getsockname()
            except:
                pass
            try:
                self.remoteaddr = self.socket.getpeername()
            except:
                pass
            self.connmark = 0
            self.connected = True
            for m in self.protocol.init(self):
                yield m
            connection_control = ConnectionControlEvent.createMatcher(self, None, True, _ismatch = lambda x: x.connmark == self.connmark or x.connmark < 0)
            while self.connected or self.need_reconnect:
                if self.connected:
                    self.connrecv = 0
                    self.connsend = 0
                    self.writestop = False
                    self.readstop = False
                    self.subroutine(self._read_main(), True, 'readroutine', self.daemon)
                    self.subroutine(self._write_main(), True, 'writeroutine', self.daemon)
                    err_match = PollEvent.createMatcher(self.socket.fileno(), PollEvent.ERROR)
                    yield (err_match, connection_control)
                    if self.matcher is connection_control and self.event.type is not ConnectionControlEvent.HANGUP:
                        if self.event.type == ConnectionControlEvent.SHUTDOWN:
                            self.need_reconnect = False
                        elif self.event.type == ConnectionControlEvent.RECONNECT:
                            self.need_reconnect = True
                        else:
                            self.need_reconnect = self.persist
                        self.logger.debug('Connection shutdown request received')
                    else:
                        self.logger.debug('Connection is closed')
                    self.connected = False
                    self.readroutine.close()
                    self.writeroutine.close()
                    if self.matcher is err_match and (self.event.detail & POLLING_ERR):
                        for m in self.protocol.error(self):
                            yield m
                    else:
                        for m in self.protocol.closed(self):
                            yield m
                if self.need_reconnect:
                    self.logger.debug('Try reconnecting')
                    for m in self._reconnect():
                        yield m
                    if self.connected:
                        try:
                            self.localaddr = self.socket.getsockname()
                        except:
                            pass
                        try:
                            self.remoteaddr = self.socket.getpeername()
                        except:
                            pass
                        self.connmark += 1
                        for m in self.protocol.reconnect_init(self):
                            yield m
                    else:
                        break
        finally:
            need_close = self.connected
            self._close()
            self.subroutine(self._final(need_close), False)
    def _final(self, need_close = False):
        if need_close:
            self.logger.debug('System is quitting, close connection, call protocol.closed()')
            for m in self.protocol.closed(self):
                yield m
        for m in self.protocol.final(self):
            yield m
    def shutdown(self, force = False, connmark = -1):
        '''
        Can call without delegate
        '''
        if connmark is None:
            connmark = self.connmark
        for m in self.waitForSend(ConnectionControlEvent(self, ConnectionControlEvent.SHUTDOWN, force, connmark)):
            yield m
    def reconnect(self, force = True, connmark = None):
        '''
        Can call without delegate
        '''
        if connmark is None:
            connmark = self.connmark
        for m in self.waitForSend(ConnectionControlEvent(self, ConnectionControlEvent.RECONNECT, force, connmark)):
            yield m
    def reset(self, force = True, connmark = None):
        '''
        Can call without delegate
        '''
        if connmark is None:
            connmark = self.connmark
        for m in self.waitForSend(ConnectionControlEvent(self, ConnectionControlEvent.RESET, force, connmark)):
            yield m
    def write(self, event, ignoreException = True):
        '''
        Can call without delegate
        '''
        connmark = self.connmark
        if self.connected:
            for m in self.waitForSend(event):
                yield m
                if not self.connected or self.connmark != connmark:
                    if not ignoreException:
                        raise ConnectionResetException
                    else:
                        return
        else:
            if not ignoreException:
                raise ConnectionResetException
    def __repr__(self, *args, **kwargs):
        baserepr = RoutineContainer.__repr__(self, *args, **kwargs)
        return baserepr + '(%r -> %r)' % (getattr(self, 'localaddr', None), getattr(self, 'remoteaddr', None))
    

@withIndices('request')
class ResolveRequestEvent(Event):
    canignore = False

@withIndices('request')
class ResolveResponseEvent(Event):
    pass

class Client(Connection):
    '''
    A single connection to a specified target
    '''
    def __init__(self, url, protocol, scheduler = None, key = None, certificate = None, ca_certs = None, bindaddress = None):
        Connection.__init__(self, protocol, None, scheduler)
        self.rawurl = url
        self.url = urlsplit(url, 'tcp')
        if self.url.scheme == 'ptcp':
            self.udp = False
            self.passive = True
            self.ssl = False
            self.unix = False
        elif self.url.scheme == 'unix':
            self.udp = False
            self.passive = False
            self.ssl = False
            self.unix = True
        elif self.url.scheme == 'punix':
            self.udp = False
            self.passive = True
            self.ssl = False
            self.unix = True
        elif self.url.scheme == 'ssl':
            self.udp = False
            self.passive = False
            self.ssl = True
            self.unix = False
        elif self.url.scheme == 'pssl':
            self.udp = False
            self.passive = True
            self.ssl = True
            self.unix = False
        elif self.url.scheme == 'udp':
            self.udp = True
            self.passive = False
            self.ssl = False
            self.unix = False
        elif self.url.scheme == 'pudp':
            self.udp = True
            self.passive = True
            self.ssl = False
            self.unix = False
        elif self.url.scheme == 'dunix':
            self.udp = True
            self.passive = False
            self.ssl = False
            self.unix = True
        elif self.url.scheme == 'pdunix':
            self.udp = True
            self.passive = True
            self.ssl = False
            self.unix = True
        else:
            self.udp = False
            self.passive = False
            self.ssl = False
            self.unix = False
        if not self.unix and not self.passive and not self.url.hostname:
            raise ValueError('Target address is not specified in url: ' + url)
        if self.unix and not self.url.path:
            raise ValueError('Unix socket path is not specified in url: ' + url)
        if not self.unix:
            self.hostname = self.url.hostname
            if not self.url.port:
                if self.ssl:
                    self.port = getattr(self.protocol, 'ssldefaultport', self.protocol.defaultport)
                else:
                    self.port = self.protocol.defaultport
            else:
                self.port = self.url.port
        else:
            self.path = self.url.path
        self.key = key
        self.certificate = certificate
        self.ca_certs = ca_certs
        self.bindaddress = bindaddress
        self.connect_timeout = getattr(self.protocol, 'connect_timeout', 30)
        self.reconnect_timeseq = getattr(self.protocol, 'reconnect_timeseq', self.defaultTimeSeq)
        self.reuseport = getattr(self.protocol, 'reuseport', False)
        self.nodelay = getattr(self.protocol, 'tcp_nodelay', False)
        self.logContext['url'] = self.rawurl
        if self.ssl:
            self.sslversion = getattr(self.protocol, 'sslversion', ssl.PROTOCOL_SSLv23)
    @staticmethod
    def defaultTimeSeq():
        nextSeq = 1
        while True:
            yield nextSeq                
            if nextSeq < 30:
                nextSeq = nextSeq * 2
            elif nextSeq < 270:
                nextSeq = nextSeq + 30
            else:
                nextSeq = 300
    def create_socket(self):
        if self.socket is not None:
            self.scheduler.unregisterPolling(self.socket)
            self.socket.close()
            self.socket = None            
        if not self.unix and self.hostname:
            request = (self.hostname, None if self.passive else self.port, socket.AF_UNSPEC, socket.SOCK_DGRAM if self.udp else socket.SOCK_STREAM, socket.IPPROTO_UDP if self.udp else socket.IPPROTO_TCP, socket.AI_ADDRCONFIG)
            # Resolve hostname
            for m in self.waitForSend(ResolveRequestEvent(request)):
                yield m
            for m in self.waitWithTimeout(self.connect_timeout, ResolveResponseEvent.createMatcher(request)):
                yield m
            if self.timeout:
                # Resolve is only allowed through asynchronous resolver
                #try:
                #    self.addrinfo = socket.getaddrinfo(self.hostname, self.port, socket.AF_UNSPEC, socket.SOCK_DGRAM if self.udp else socket.SOCK_STREAM, socket.IPPROTO_UDP if self.udp else socket.IPPROTO_TCP, socket.AI_ADDRCONFIG|socket.AI_NUMERICHOST)
                #except:
                raise IOError('Resolve hostname timeout: ' + self.hostname)
            else:
                if hasattr(self.event, 'error'):
                    raise IOError('Cannot resolve hostname: ' + self.hostname)
                self.addrinfo = self.event.response
        if self.passive:
            if self.unix:
                socket_listen = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM if self.udp else socket.SOCK_STREAM)
                socket_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if self.reuseport:
                    socket_listen.setsockopt(socket.SOL_SOCKET, 15, 1)                    
                self.logger.debug('Bind unix socket to %s', self.path)
                socket_listen.bind(self.path)
                self.valid_addresses = None
                family = socket.AF_UNIX
            elif self.hostname:
                if self.bindaddress is not None:
                    bind_protos = set(b[0] for b in self.bindaddress)
                else:
                    bind_protos = set((socket.AF_INET,socket.AF_INET6))
                addrinfo_protos = set(addr[0] for addr in self.addrinfo)
                if socket.AF_INET in bind_protos and socket.AF_INET in addrinfo_protos:
                    family = socket.AF_INET
                elif socket.AF_INET6 in bind_protos and socket.AF_INET6 in addrinfo_protos:
                    family = socket.AF_INET6
                else:
                    raise ValueError('Target address has different address family with local bind address')
                socket_listen = socket.socket(family, socket.SOCK_DGRAM if self.udp else socket.SOCK_STREAM)
                socket_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if not self.udp and self.nodelay:
                    socket_listen.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
                if self.reuseport:
                    socket_listen.setsockopt(socket.SOL_SOCKET, 15, 1)                    
                if self.bindaddress is not None:
                    for b in self.bindaddress:
                        if b[0] == family:
                            socket_listen.bind((b[1], self.port))
                            break
                else:
                    if family == socket.AF_INET:
                        socket_listen.bind(('0.0.0.0',self.port))
                    else:
                        socket_listen.bind(('::',self.port))
                self.valid_addresses = set(addr[4][0] for addr in self.addrinfo if addr[0] == family)
            else:
                if self.bindaddress is not None:
                    bind_protos = set(b[0] for b in self.bindaddress)
                else:
                    bind_protos = set((socket.AF_INET,socket.AF_INET6))
                if socket.AF_INET in bind_protos:
                    family = socket.AF_INET
                elif socket.AF_INET6 in bind_protos:
                    family = socket.AF_INET6
                else:
                    raise ValueError('Local bind address is invalid')
                socket_listen = socket.socket(family, socket.SOCK_DGRAM if self.udp else socket.SOCK_STREAM)
                socket_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if not self.udp and self.nodelay:
                    socket_listen.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
                if self.reuseport:
                    socket_listen.setsockopt(socket.SOL_SOCKET, 15, 1)                    
                if self.bindaddress is not None:
                    for b in self.bindaddress:
                        if b[0] == family:
                            socket_listen.bind((b[1], self.port))
                            break
                else:
                    if family == socket.AF_INET:
                        socket_listen.bind(('0.0.0.0',self.port))
                    else:
                        socket_listen.bind(('::',self.port))
                self.valid_addresses = None
            if self.udp:
                try:
                    self.socket = socket_listen
                    self.socket.setblocking(False)
                    self.scheduler.registerPolling(self.socket)
                    read_udp = PollEvent.createMatcher(self.socket.fileno(), PollEvent.READ_READY)
                    err_udp = PollEvent.createMatcher(self.socket.fileno(), PollEvent.ERROR)
                    connected = False
                    while not connected:
                        yield (read_udp, err_udp)
                        if self.event.category == PollEvent.READ_READY:
                            while not connected:
                                try:
                                    data, remote_addr = self.socket.recvfrom(65536, socket.MSG_PEEK)
                                    self.logger.debug('Udp socket receive data from %s', remote_addr)
                                except socket.error as exc:
                                    if exc.args[0] == errno.EWOULDBLOCK or exc.args[0] == errno.EAGAIN:
                                        break
                                    else:
                                        raise
                                if self.valid_addresses is not None:
                                    if remote_addr[0] not in self.valid_addresses:
                                        self.socket.recvfrom(1)
                                    else:
                                        connected = True
                                else:
                                    connected = True
                        elif self.event.category == PollEvent.ERROR or self.event.category == PollEvent.HANGUP:
                            raise IOError('Listen socket is closed')
                    try:
                        err = self.socket.connect_ex(remote_addr)
                        if err == errno.EINPROGRESS:
                            connect_match = PollEvent.createMatcher(self.socket.fileno())
                            for m in self.waitWithTimeout(self.connect_timeout, connect_match):
                                yield m
                            if self.timeout:
                                raise IOError('timeout')
                            else:
                                err = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                                if err != 0:
                                    raise IOError('socket error: ' + str(err))
                        elif err != 0:
                            raise IOError('socket error: ' + str(err))
                    except:
                        self.logger.debug('Failed to connect to remote address: ' + repr(addr), exc_info = True)
                        self.scheduler.unregisterPolling(self.socket)
                        self.socket.close()
                        self.socket = None
                        raise
                finally:
                    if self.unix:
                        try:
                            os.remove(self.path)
                        except Exception:
                            pass
            else:
                try:
                    socket_listen.setblocking(False)
                    self.scheduler.registerPolling(socket_listen, POLLING_IN)
                    socket_listen.listen(8)
                    m = PollEvent.createMatcher(socket_listen.fileno())
                    connected = False
                    while not connected:
                        yield (m,)
                        if self.event.category == PollEvent.READ_READY:
                            while not connected:
                                try:
                                    self.socket, remote_addr = socket_listen.accept()
                                except socket.error as exc:
                                    if exc.args[0] == errno.EWOULDBLOCK or exc.args[0] == errno.EAGAIN:
                                        break
                                    else:
                                        raise
                                if self.valid_addresses is not None:
                                    if remote_addr[0] not in self.valid_addresses:
                                        self.socket.close()
                                    else:
                                        connected = True
                                else:
                                    connected = True
                        elif self.event.category == PollEvent.ERROR and self.event.category == PollEvent.HANGUP:
                            raise IOError('Listen socket is closed')
                finally:
                    self.scheduler.unregisterPolling(socket_listen)
                    socket_listen.close()
                    if self.unix:
                        try:
                            os.remove(self.path)
                        except Exception:
                            pass
                self.socket.setblocking(False)
                self.scheduler.registerPolling(self.socket)
        else:
            if self.unix:
                family = socket.AF_UNIX
                self.addresses = (self.path,)
            else:
                if self.bindaddress is not None:
                    bind_protos = set(b[0] for b in self.bindaddress)
                else:
                    bind_protos = set((socket.AF_INET,socket.AF_INET6))
                addrinfo_protos = set(addr[0] for addr in self.addrinfo)
                if socket.AF_INET in bind_protos and socket.AF_INET in addrinfo_protos:
                    family = socket.AF_INET
                elif socket.AF_INET6 in bind_protos and socket.AF_INET6 in addrinfo_protos:
                    family = socket.AF_INET6
                else:
                    raise ValueError('Target address has different address family with local bind address')
                self.addresses = tuple(addr[4] for addr in self.addrinfo if addr[0] == family)
            for addr in self.addresses:
                self.socket = None
                try:
                    self.socket = socket.socket(family, socket.SOCK_DGRAM if self.udp else socket.SOCK_STREAM)
                except:
                    self.logger.debug('Failed to create socket for family: ' + repr(family), exc_info = True)
                    continue
                if not self.unix and not self.udp and self.nodelay:
                    self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
                try:
                    if self.bindaddress is not None:
                        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        for b in self.bindaddress:
                            if b[0] == family:
                                self.logger.debug('Bind socket to address: %s', b[1])
                                self.socket.bind(b[1])
                                break
                    elif self.udp:
                        if family == socket.AF_INET:
                            self.socket.bind(('0.0.0.0',0))
                        elif family == socket.AF_INET6:
                            self.socket.bind(('::',0))
                    self.socket.setblocking(False)
                    self.scheduler.registerPolling(self.socket)
                except:
                    self.logger.debug('Failed to bind to local address: ' + repr(self.bindaddress), exc_info = True)
                    self.socket.close()
                    self.socket = None
                    continue
                try:
                    err = self.socket.connect_ex(addr)
                    if err == errno.EINPROGRESS or err == errno.EWOULDBLOCK or err == errno.EAGAIN:
                        connect_match = PollEvent.createMatcher(self.socket.fileno())
                        for m in self.waitWithTimeout(self.connect_timeout, connect_match):
                            yield m
                        if self.timeout:
                            raise Exception('timeout')
                        else:
                            err = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                            if err == 0:
                                break
                            else:
                                raise Exception('socket error: ' + errno.errorcode.get(err, str(err)))
                    elif err != 0:
                        raise Exception('socket error: ' + errno.errorcode.get(err, str(err)))
                    else:
                        break
                except:
                    self.logger.debug('Failed to connect to remote address: ' + repr(addr), exc_info = True)
                    self.scheduler.unregisterPolling(self.socket)
                    self.socket.close()
                    self.socket = None
                    continue
        if self.socket is None:
            raise IOError('Cannot create connection')
        if self.ssl:
            try:
                self.logger.debug('Wrapping socket with SSL Context')
                self.socket = ssl.wrap_socket(self.socket, self.key, self.certificate, self.passive, ssl.CERT_NONE if self.ca_certs is None else ssl.CERT_REQUIRED,
                                              self.sslversion, self.ca_certs, False)
                handshake = False
                read_matcher = PollEvent.createMatcher(self.socket.fileno(), PollEvent.READ_READY)
                write_matcher = PollEvent.createMatcher(self.socket.fileno(), PollEvent.WRITE_READY)
                error_matcher = PollEvent.createMatcher(self.socket.fileno(), PollEvent.ERROR)
                while not handshake:
                    try:
                        self.logger.debug('Doing handshake')
                        self.socket.do_handshake()
                        handshake = True
                    except ssl.SSLError as exc:
                        if exc.args[0] == ssl.SSL_ERROR_WANT_READ:
                            yield (read_matcher, error_matcher)
                            if self.matcher is error_matcher:
                                raise IOError('Socket closed or get error status before SSL handshake complete')
                        elif exc.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                            yield (write_matcher, error_matcher)
                            if self.matcher is error_matcher:
                                raise IOError('Socket closed or get error status before SSL handshake complete')
                        else:
                            raise
            except:
                self.scheduler.unregisterPolling(self.socket)
                self.socket.close()
                self.socket = None
                raise
    def _reconnect_internal(self):
        timeretry = 0
        for timewait in self.reconnect_timeseq():
            for m in self.waitWithTimeout(timewait):
                yield m
            try:
                with closing(self.create_socket()) as cs:
                    for m in cs:
                        yield m
                break
            except IOError as exc:
                timeretry += 1
                if timeretry > 3:
                    self.logger.warning('Reconnect failed after %d times retry, url=%s', timeretry, self.rawurl, exc_info = True)
    def _reconnect(self):
        matcher = ConnectionControlEvent.createMatcher(self, ConnectionControlEvent.SHUTDOWN, _ismatch = lambda x: x.connmark == self.connmark or x.connmark < 0)
        try:
            self.connected = False
            with closing(self.withException(self._reconnect_internal(), matcher)) as g:
                for m in g:
                    yield m
            self.connected = True
        except RoutineException:
            self.need_reconnect = False
            self.connected = False
    def main(self):
        self.connmark = -1
        try:
            matcher = ConnectionControlEvent.createMatcher(self, ConnectionControlEvent.SHUTDOWN, _ismatch = lambda x: x.connmark == self.connmark or x.connmark < 0)
            with closing(self.withException(self.create_socket(), matcher)) as g:
                for m in g:
                    yield m
        except RoutineException:
            return
        except IOError:
            self.logger.warning('Connection failed for url: %s', self.rawurl, exc_info = True)
            if self.need_reconnect:
                with closing(self._reconnect()) as g:
                    for m in g:
                        yield m
                if not self.connected:
                    for m in self.protocol.notconnected(self):
                        yield m
            else:
                for m in self.protocol.notconnected(self):
                    yield m
        if self.socket:
            with closing(Connection.main(self)) as g:
                for m in g:
                    yield m
    def __repr__(self, *args, **kwargs):
        return Connection.__repr__(self, *args, **kwargs) + '(url=' + self.rawurl + ')'

class TcpServer(RoutineContainer):
    '''
    A server receiving multiple connections
    '''
    logger = logging.getLogger(__name__ + '.TcpServer')
    def __init__(self, url, protocol, scheduler = None, key = None, certificate = None, ca_certs = None):
        RoutineContainer.__init__(self, scheduler)
        self.protocol = protocol
        self.rawurl = url
        self.url = urlsplit(url, 'tcp')
        if self.url.scheme == 'unix' or self.url.scheme == 'lunix':
            self.udp = False
            self.passive = True
            self.ssl = False
            self.unix = True
        elif self.url.scheme == 'ssl' or self.url.scheme == 'lssl':
            self.udp = False
            self.passive = True
            self.ssl = True
            self.unix = False
        else:
            self.udp = False
            self.passive = True
            self.ssl = False
            self.unix = False
        if self.unix and not self.url.path:
            raise ValueError('Unix socket path is not specified in url: ' + url)
        if not self.unix:
            self.hostname = self.url.hostname
            if not self.url.port:
                self.port = self.protocol.defaultport
            else:
                self.port = self.url.port
        else:
            self.path = self.url.path
        self.key = key
        self.certificate = certificate
        self.ca_certs = ca_certs
        self.retry_listen = getattr(self.protocol, 'listen_persist', True)
        self.retry_interval = getattr(self.protocol, 'retrylisten_interval', 3)
        if self.ssl:
            self.sslversion = getattr(self.protocol, 'sslversion', ssl.PROTOCOL_SSLv23)
        self.backlogsize = getattr(self.protocol, 'backlogsize', 2048)
        self.reuseport = getattr(self.protocol, 'reuseport', False)
        self.nodelay = getattr(self.protocol, 'nodelay', False)
        self.logger = ContextAdapter(self.logger, {'context':{'server':self, 'url':self.rawurl, 'protocol':protocol}})
        # Counters
        self.totalaccepts = 0
        self.accepts = 0
        self.listening = False
    def _connection(self, newsock, newproto):
        try:
            newsock.setblocking(False)
            if self.nodelay:
                newsock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            self.scheduler.registerPolling(newsock, POLLING_IN|POLLING_OUT)
            if self.ssl:
                handshake = False
                read_matcher = PollEvent.createMatcher(newsock.fileno(), PollEvent.READ_READY)
                write_matcher = PollEvent.createMatcher(newsock.fileno(), PollEvent.WRITE_READY)
                error_matcher = PollEvent.createMatcher(newsock.fileno(), PollEvent.ERROR)
                while not handshake:
                    try:
                        self.logger.debug('Doing handshake on accepted socket: %d', newsock.fileno())
                        newsock.do_handshake()
                        handshake = True
                    except ssl.SSLError as exc:
                        if exc.args[0] == ssl.SSL_ERROR_WANT_READ:
                            yield (read_matcher, error_matcher)
                            if self.matcher is error_matcher:
                                raise IOError('Socket closed or get error status before SSL handshake complete')
                        elif exc.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                            yield (write_matcher, error_matcher)
                            if self.matcher is error_matcher:
                                raise IOError('Socket closed or get error status before SSL handshake complete')
                        else:
                            raise
            conn = Connection(newproto, newsock, self.scheduler)
            conn.need_reconnect = False
            conn.start()
            self.accepts += 1
            self.totalaccepts += 1
        except:
            self.scheduler.unregisterPolling(newsock)
            newsock.close()
            raise
    def _server(self):
        if not self.unix:
            request = (None if not self.hostname else self.hostname, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP, socket.AI_ADDRCONFIG|socket.AI_PASSIVE)
            # Resolve hostname
            for m in self.waitForSend(ResolveRequestEvent(request)):
                yield m
            for m in self.waitWithTimeout(20, ResolveResponseEvent.createMatcher(request)):
                yield m
            if self.timeout:
                # Resolve is only allowed through asynchronous resolver 
                self.addrinfo = socket.getaddrinfo(self.hostname, self.port, socket.AF_UNSPEC, socket.SOCK_DGRAM if self.udp else socket.SOCK_STREAM, socket.IPPROTO_UDP if self.udp else socket.IPPROTO_TCP, socket.AI_ADDRCONFIG|socket.AI_NUMERICHOST)
            else:
                if hasattr(self.event, 'error'):
                    raise IOError('Cannot resolve hostname: ' + self.hostname)
                self.addrinfo = self.event.response
        if self.unix:
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if self.reuseport:
                self.socket.setsockopt(socket.SOL_SOCKET, 15, 1)
            self.socket.bind(self.path)
            family = socket.AF_UNIX
        else:
            addrinfo_protos = set(addr[0] for addr in self.addrinfo)
            if socket.AF_INET in addrinfo_protos:
                family = socket.AF_INET
            elif socket.AF_INET6 in addrinfo_protos:
                family = socket.AF_INET6
            else:
                raise ValueError('Local bind address is invalid')
            self.socket = socket.socket(family, socket.SOCK_STREAM)
            if self.nodelay:
                self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if self.reuseport:
                self.socket.setsockopt(socket.SOL_SOCKET, 15, 1)
            for b in self.addrinfo:
                if b[0] == family:
                    self.socket.bind(b[4])
                    break
        try:
            self.socket.setblocking(False)
            if self.ssl:
                self.socket = ssl.wrap_socket(self.socket, self.key, self.certificate, True, ssl.CERT_NONE if self.ca_certs is None else ssl.CERT_REQUIRED,
                                              self.sslversion, self.ca_certs, False)                
            for m in self.protocol.beforelisten(self, self.socket):
                yield m
            self.scheduler.registerPolling(self.socket, POLLING_IN)
            self.socket.listen(self.backlogsize)
            self.listening = True
            try:
                self.localaddr = self.socket.getsockname()
            except:
                pass
            m = PollEvent.createMatcher(self.socket.fileno())
            while True:
                yield (m,)
                if self.event.category == PollEvent.READ_READY:
                    while True:
                        try:
                            new_socket, remote_addr = self.socket.accept()
                        except socket.error as exc:
                            if exc.errno == errno.EWOULDBLOCK or exc.errno == errno.EAGAIN:
                                break
                            else:
                                self.logger.warning('Unexpected exception on accepting', exc_info = True)
                                break
                        try:
                            if hasattr(self.protocol, 'accept'):
                                new_proto = self.protocol.accept(self, remote_addr, new_socket)
                                if new_proto is None:
                                    new_socket.close()
                                    continue
                            else:
                                new_proto = self.protocol
                            self.subroutine(self._connection(new_socket, new_proto)) 
                        except:
                            new_socket.close() 
                else:
                    self.logger.warning('Error polling status received: ' + repr(self.event))
                    break
        finally:
            self.scheduler.unregisterPolling(self.socket)
            self.socket.close()
            if self.unix:
                try:
                    os.remove(self.path)
                except Exception:
                    pass
            self.listening = False
    def main(self):
        self.connmark = 0
        matcher = ConnectionControlEvent.createMatcher(self, ConnectionControlEvent.SHUTDOWN, _ismatch = lambda x: x.connmark == self.connmark or x.connmark < 0)
        matcher2 = ConnectionControlEvent.createMatcher(self, ConnectionControlEvent.STOPLISTEN, _ismatch = lambda x: x.connmark == self.connmark or x.connmark < 0)
        matcher3 = ConnectionControlEvent.createMatcher(self, ConnectionControlEvent.STARTLISTEN, _ismatch = lambda x: x.connmark == self.connmark or x.connmark < 0)
        retry = True
        self.retry_interval = None
        try:
            while retry:
                self.connmark += 1
                self.accepts = 0
                try:
                    with closing(self.withException(self._server(), matcher, matcher2)) as g:
                        for m in g:
                            yield m
                except RoutineException:
                    if self.matcher is matcher:
                        retry = False
                    else:
                        yield (matcher, matcher3)
                        if self.matcher is matcher:
                            retry = False
                        else:
                            retry = True
                except IOError:
                    retry = self.retry_listen
                    if retry:
                        self.logger.warning('Begin listen failed on URL: %s', self.rawurl, exc_info = True)
                        for m in self.waitWithTimeout(self.retry_interval):
                            yield m
                    else:
                        raise
        finally:
            self.subroutine(self._final())
    def shutdown(self, connmark = -1):
        '''
        Can call without delegate
        '''
        if connmark is None:
            connmark = self.connmark
        for m in self.waitForSend(ConnectionControlEvent(self, ConnectionControlEvent.SHUTDOWN, True, connmark)):
            yield m
    def stoplisten(self, connmark = -1):
        '''
        Can call without delegate
        '''
        if connmark is None:
            connmark = self.connmark
        for m in self.waitForSend(ConnectionControlEvent(self, ConnectionControlEvent.STOPLISTEN, True, connmark)):
            yield m    
    def startlisten(self, connmark = -1):
        '''
        Can call without delegate
        '''
        if connmark is None:
            connmark = self.connmark
        for m in self.waitForSend(ConnectionControlEvent(self, ConnectionControlEvent.STARTLISTEN, True, connmark)):
            yield m    
    def _final(self):
        for m in self.protocol.serverfinal(self):
            yield m
    def __repr__(self, *args, **kwargs):
        baserepr = RoutineContainer.__repr__(self, *args, **kwargs)
        return baserepr + '(Listen on %r)' % (getattr(self, 'localaddr', None),)
