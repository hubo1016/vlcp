'''
Created on 2016/9/13

:author: think
'''

from vlcp.protocol.protocol import Protocol
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices, ConnectionWriteEvent
from collections import deque
from vlcp.utils.zookeeper import ZooKeeperRequest, ZooKeeperReply, CONNECT_PACKET, HEADER_PACKET,\
        WATCHER_EVENT_XID
import vlcp.utils.zookeeper as zk
import logging
import os
from vlcp.event.connection import ConnectionResetException
from contextlib import closing

@withIndices('state', 'connection', 'connmark', 'createby')
class ZooKeeperConnectionStateEvent(Event):
    UP = 'up'
    DOWN = 'down'
    NOTCONNECTED = 'notconnected'

@withIndices('connection', 'connmark', 'createby')
class ZooKeeperMessageEvent(Event):
    pass

@withIndices('xid')
class ZooKeeperResponseEvent(ZooKeeperMessageEvent):
    pass

@withIndices('type', 'state', 'path')
class ZooKeeperWatcherEvent(ZooKeeperMessageEvent):
    pass

@withIndices()
class ZooKeeperHandshakeEvent(ZooKeeperMessageEvent):
    pass

class ZooKeeperProtocolException(Exception):
    pass

class ZooKeeperException(Exception):
    pass

class ZooKeeperRetryException(ZooKeeperException):
    '''
    Connection lost or not connected on handshake
    '''
    pass

class ZooKeeperSessionExpiredException(ZooKeeperException):
    '''
    Handshake reports the session is expired
    '''
    pass

class ZooKeeperRequestTooLargeException(ZooKeeperException):
    '''
    Request is too large, which may break every thing, so we reject it
    '''
    pass

@withIndices('priority')
class ZooKeeperWriteEvent(ConnectionWriteEvent):
    LOW = 0
    MIDDLE = 1
    HIGH = 2

@defaultconfig
class ZooKeeper(Protocol):
    '''
    ZooKeeper protocol
    '''
    # Usually we should connect to another server, this is done by ZooKeeperClient
    _default_persist = False
    # default ZooKeeper port
    _default_defaultport = 2181
    _default_createqueue = True
    _default_buffersize = 4194304
    # Limit the response and watcher queue size
    _default_messagequeuesize = 1024
    # Send ping command when the connection is idle
    _default_keepalivetime = 5
    # Disconnect when the ping command does not receive response
    _default_keepalivetimeout = 10
    _default_connect_timeout = 5
    _default_tcp_nodelay = True
    # Limit the data write queue size
    _default_writequeuesize = 8192
    _logger = logging.getLogger(__name__ + '.ZooKeeper')
    def init(self, connection):
        for m in Protocol.init(self, connection):
            yield m
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, ZooKeeperConnectionStateEvent.createMatcher(connection = connection), ('connstate', connection)))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority + 1, ZooKeeperMessageEvent.createMatcher(connection = connection), ('message', connection), self.messagequeuesize))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority + 2, ZooKeeperResponseEvent.createMatcher(connection = connection, xid = zk.PING_XID)))
        if hasattr(connection, 'queue'):
            connection.queue.addSubQueue(1, ZooKeeperWriteEvent.createMatcher(connection = connection,
                                                                                        priority = ZooKeeperWriteEvent.MIDDLE),
                                         None, self.writequeuesize)
            connection.queue.addSubQueue(2, ZooKeeperWriteEvent.createMatcher(connection = connection,
                                                                                        priority = ZooKeeperWriteEvent.HIGH),
                                         None, self.writequeuesize)
        for m in self.reconnect_init(connection):
            yield m
    def reconnect_init(self, connection):
        for m in Protocol.reconnect_init(self, connection):
            yield m
        connection.xid = ord(os.urandom(1)) + 1
        connection.zookeeper_requests = {}
        connection.zookeeper_handshake = False
        connection.zookeeper_lastzxid = 0
        for m in connection.waitForSend(ZooKeeperConnectionStateEvent(ZooKeeperConnectionStateEvent.UP,
                                                                      connection,
                                                                      connection.connmark,
                                                                      self)):
            yield m
    def serialize(self, connection, event):
        event._zookeeper_sent = True
        return Protocol.serialize(self, connection, event)
    def parse(self, connection, data, laststart):
        events = []
        start = 0
        while True:
            result = ZooKeeperReply.parse(data[start:])
            if result is None:
                break
            reply, size = result
            start += size
            if not connection.zookeeper_handshake:
                reply.zookeeper_type = CONNECT_PACKET
                reply._autosubclass()
                connection.zookeeper_handshake = True
                events.append(ZooKeeperHandshakeEvent(connection, connection.connmark, self, message = reply))
            else:
                reply.zookeeper_type = HEADER_PACKET
                reply._autosubclass()
                if reply.zxid > 0:
                    connection.zookeeper_lastzxid = reply.zxid
                if reply.xid >= 0:
                    xid = reply.xid
                    if xid not in connection.zookeeper_requests:
                        raise ZooKeeperProtocolException('xid does not match: receive %r' % (reply.xid,))
                    request_type = connection.zookeeper_requests.pop(xid)
                    reply.zookeeper_request_type = request_type
                    reply._autosubclass()
                if reply.xid == WATCHER_EVENT_XID:
                    events.append(ZooKeeperWatcherEvent(connection, connection.connmark,
                                                        self, reply.type, reply.state, b'' if reply.path is None else reply.path,
                                                        message = reply))
                else:
                    events.append(ZooKeeperResponseEvent(connection, connection.connmark,
                                                         self, reply.xid, message = reply))
        if laststart == len(data):
            # Remote write close
            events.append(ConnectionWriteEvent(connection, connection.connmark, data = b'', EOF = True))
        return (events, len(data) - start)
    def _send(self, connection, request, container, priority = 0):
        return self._senddata(connection, request._tobytes(), container, priority)
    def _senddata(self, connection, data, container, priority = 0):
        connwrite = ZooKeeperWriteEvent(connection, connection.connmark, priority, data = data)
        connwrite._zookeeper_sent = False
        try:
            for m in connection.write(connwrite, False):
                yield m
        except ConnectionResetException:
            raise ZooKeeperRetryException
        container.retvalue = connwrite
    def _pre_assign_xid(self, connection, request):
        if hasattr(request, 'xid') and request.xid >= 0:
            connection.xid += 1
            if connection.xid > 0x7fffffff:
                connection.xid = 1
            request.xid = connection.xid
        return request.xid
    def _register_xid(self, connection, request):
        if hasattr(request, 'xid') and request.xid >= 0:
            connection.zookeeper_requests[request.xid] = request.type
    def handshake(self, connection, connrequest, container, extrapackets = []):
        connmark = connection.connmark
        handshake_matcher = ZooKeeperHandshakeEvent.createMatcher(connection, connection.connmark)
        for m in self._send(connection, connrequest, container, ZooKeeperWriteEvent.HIGH):
            yield m
        handshake_received = [None]
        if extrapackets:
            def callback(event, matcher):
                handshake_received[0] = event.message
            with closing(container.executeWithTimeout(10,
                            container.withCallback(
                                self.requests(connection, extrapackets, container, priority=ZooKeeperWriteEvent.HIGH),
                                callback, handshake_matcher))) as g:
                for m in g:
                    yield m
            if container.timeout:
                raise ZooKeeperRetryException
            receive, lost, retry = container.retvalue
            if lost or retry:
                if handshake_received[0] is not None:
                    # We have received the handshake packet, but not the extra responses
                    # Maybe the session is expired
                    if handshake_received[0].timeOut <= 0:
                        raise ZooKeeperSessionExpiredException
                raise ZooKeeperRetryException
        else:
            receive = []
        if handshake_received[0] is None:
            if not connection.connected or connection.connmark != connmark:
                raise ZooKeeperRetryException
            else:
                conn_matcher = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.DOWN,
                                                                           connection,
                                                                           connmark)
                for m in container.waitWithTimeout(10, handshake_matcher, conn_matcher):
                    yield m
                if container.timeout:
                    self._logger.warning('Handshake timeout, connection = %r', connection)
                    raise ZooKeeperRetryException
                elif container.matcher is conn_matcher:
                    raise ZooKeeperRetryException
                else:
                    handshake_received[0] = container.event.message
                    if handshake_received[0].timeOut <= 0:
                        raise ZooKeeperSessionExpiredException
        container.retvalue = (handshake_received[0], receive)
    def async_requests(self, connection, requests, container, priority = 0):
        '''
        :return: (matchers, generator), where matchers are event matchers for the requests; generator
                 is the routine to send to requests.
        '''
        matchers = []
        for r in requests:
            xid = self._pre_assign_xid(connection, r)
            resp_matcher = ZooKeeperResponseEvent.createMatcher(connection, connection.connmark, None, xid)
            matchers.append(resp_matcher)
        alldata = []
        for r in requests:
            data = r._tobytes()
            if len(data) >= 0xfffff:
                # This is the default limit of ZooKeeper, reject this request
                raise ZooKeeperRequestTooLargeException('The request is %d bytes which is too large for ZooKeeper' % len(data))
            alldata.append(data)
        for r in requests:
            self._register_xid(connection, r)
        def _sendall():
            sent_requests = []
            for data in alldata:
                try:
                    for m in self._senddata(connection, data, container, priority):
                        yield m
                    sent_requests.append(container.retvalue)
                except ZooKeeperRetryException:
                    raise ZooKeeperRetryException(sent_requests)
            container.retvalue = sent_requests
        return (matchers, _sendall())
    def requests(self, connection, requests, container, callback = None, priority = 0):
        '''
        Send requests by sequence, return all the results (including the lost ones)
        
        :params connection: ZooKeeper connection
        
        :params requests: a sequence of ZooKeeper requests
        
        :params container: routine container of current routine
        
        :params callback: if not None, `callback(request, response)` is called immediately after
                          each response received
        
        :return: `(responses, lost_responses, retry_requests)`, where responses is a list of responses corresponded
                    to the requests (None if response is not received); lost_responses is a list of requests that are sent
                    but the responses are lost due to connection lost, it is the caller's responsibility to determine whether
                    the call is succeeded or failed; retry_requests are the requests which are not sent and are safe to retry. 
        '''
        matchers, routine = self.async_requests(connection, requests, container, priority)
        requests_dict = dict((m,r) for m,r in zip(matchers, requests))
        connmark = connection.connmark
        conn_matcher = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.DOWN,
                                                                   connection,
                                                                   connmark)
        replydict = {}
        cr = container.currentroutine
        container.scheduler.register(tuple(matchers) + (conn_matcher,), cr)
        ms = len(matchers)
        def matcher_callback(event, matcher):
            container.scheduler.unregister((matcher,), cr)
            replydict[matcher] = event.message
            if callback:
                try:
                    callback(requests_dict[matcher], event.message)
                except:
                    container.scheduler.unregister(tuple(matchers) + (conn_matcher,), cr)
                    raise
        receive_all = True
        try:
            for m in routine:
                while True:
                    yield m
                    m2 = container.matcher
                    if m2 not in m:
                        if m2 is conn_matcher:
                            receive_all = False
                            ms = 0
                            container.scheduler.unregister(tuple(matchers) + (conn_matcher,), cr)
                        else:
                            matcher_callback(container.event, m2)
                            ms -= 1
                    else:
                        break
        except ZooKeeperRetryException as exc:
            sent_events = exc.args[0]
        else:
            sent_events = container.retvalue
        while ms:
            yield ()
            m2 = container.matcher
            if m2 is conn_matcher:
                ms = 0
                receive_all = False
                container.scheduler.unregister(tuple(matchers) + (conn_matcher,), cr)
            else:
                matcher_callback(container.event, m2)
                ms -= 1
        container.scheduler.unregister((conn_matcher,), cr)
        responses = [replydict.get(m, None) for m in matchers]
        received_responses = dict((k,v) for k,v in zip(requests, responses) if v is not None)
        if receive_all:
            container.retvalue = (responses, [], [])
        else:
            # Some results are missing
            lost_responses = [r for r,c in zip(requests, sent_events) if c._zookeeper_sent and r not in received_responses]
            retry_requests = [r for r,c in zip(requests, sent_events) if not c._zookeeper_sent] + requests[len(sent_events):]
            container.retvalue = (responses, lost_responses, retry_requests)
    def error(self, connection):
        for m in connection.waitForSend(ZooKeeperConnectionStateEvent(ZooKeeperConnectionStateEvent.DOWN,
                                                                      connection,
                                                                      connection.connmark,
                                                                      self)):
            yield m
        for m in Protocol.error(self, connection):
            yield m
    def closed(self, connection):
        for m in connection.waitForSend(ZooKeeperConnectionStateEvent(ZooKeeperConnectionStateEvent.DOWN,
                                                                      connection,
                                                                      connection.connmark,
                                                                      self)):
            yield m
        for m in Protocol.closed(self, connection):
            yield m
    def notconnected(self, connection):
        for m in connection.waitForSend(ZooKeeperConnectionStateEvent(ZooKeeperConnectionStateEvent.NOTCONNECTED,
                                                                      connection,
                                                                      connection.connmark,
                                                                      self)):
            yield m
        for m in Protocol.notconnected(self, connection):
            yield m
    def keepalive(self, connection):
        try:
            for m in connection.executeWithTimeout(self.keepalivetimeout,
                        self.requests(connection,
                            [zk.RequestHeader(xid = zk.PING_XID, type = zk.ZOO_PING_OP)],
                            connection)):
                yield m
            if connection.timeout:
                for m in connection.reset(True):
                    yield m
            else:
                _, lost, retry = connection.retvalue
                if lost or retry:
                    for m in connection.reset(True):
                        yield m
        except Exception as exc:
            for m in connection.reset(True):
                yield m
            raise exc

