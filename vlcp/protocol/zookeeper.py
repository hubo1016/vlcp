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
    Connection lost before request sent, it is safe to retry the request when reconnected.
    '''
    pass

class ZooKeeperResponseLostException(ZooKeeperException):
    '''
    Connection lost after request sent. The request may or may not be executed, sender should
    take care of this situation and design some way to determine the result.
    '''
    pass

@defaultconfig
class ZooKeeper(Protocol):
    '''
    ZooKeeper protocol
    '''
    _default_persist = False        # Usually we should connect to another server
    _default_defaultport = 2181     
    _default_createqueue = True
    _default_buffersize = 4194304
    _default_messagequeuesize = 128
    # Use hiredis if possible
    _default_keepalivetime = 10
    _default_keepalivetimeout = 10
    _default_connect_timeout = 5
    _default_tcp_nodelay = True
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
    def _send(self, connection, request, container):
        connwrite = ConnectionWriteEvent(connection, connection.connmark, data = request._tobytes())
        connwrite._zookeeper_sent = False
        try:
            for m in connection.write(connwrite, False):
                yield m
        except ConnectionResetException:
            raise ZooKeeperRetryException
        container.retvalue = connwrite
    def _assign_xid(self, connection, request):
        if hasattr(request, 'xid') and request.xid >= 0:
            connection.xid += 1
            if connection.xid > 0x7fffffff:
                connection.xid = 1
            request.xid = connection.xid
            connection.zookeeper_requests[request.xid] = request.type
        return request.xid
    def handshake(self, connection, connrequest, container, extrapackets = []):
        connmark = connection.connmark
        handshake_matcher = ZooKeeperHandshakeEvent.createMatcher(connection, connection.connmark)
        for m in self._send(connection, connrequest, container):
            yield m
        handshake_received = [None]
        if extrapackets:
            def callback(event, matcher):
                handshake_received[0] = event.message
            for m in container.withCallback(self.requests(connection, extrapackets, container), callback, handshake_matcher):
                yield m
            receive, lost, retry = container.retvalue
            if lost or retry:
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
                yield (handshake_matcher, conn_matcher)
                if container.matcher is conn_matcher:
                    raise ZooKeeperRetryException
                else:
                    handshake_received[0] = container.event.message
        container.retvalue = (handshake_received[0], receive)
    def async_requests(self, connection, requests, container):
        '''
        :return: (matchers, generator), where matchers are event matchers for the requests; generator
        is the routine to send to requests.
        '''
        matchers = []
        for r in requests:
            xid = self._assign_xid(connection, r)
            resp_matcher = ZooKeeperResponseEvent.createMatcher(connection, connection.connmark, None, xid)
            matchers.append(resp_matcher)
        def _sendall():
            sent_requests = []
            for r in requests:
                try:
                    for m in self._send(connection, r, container):
                        yield m
                    sent_requests.append(container.retvalue)
                except ZooKeeperRetryException:
                    raise ZooKeeperRetryException(sent_requests)
            container.retvalue = sent_requests
        return (matchers, _sendall())
    def requests(self, connection, requests, container, callback = None):
        '''
        Send requests by sequence, return all the results (including the lost ones)
        
        :params connection: ZooKeeper connection
        
        :params requests: a sequence of ZooKeeper requests
        
        :params container: routine container of current routine
        
        :params callback: if not None, callback(request, response) is called immediately after
        each response received
        
        :return: (responses, lost_responses, retry_requests), where responses is a list of responses corresponded
        to the requests (None if response is not received); lost_responses is a list of requests that are sent
        but the responses are lost due to connection lost, it is the caller's responsibility to determine whether
        the call is succeeded or failed; retry_requests are the requests which are not sent and are safe to retry. 
        '''
        matchers, routine = self.async_requests(connection, requests, container)
        requests_dict = dict((m,r) for m,r in zip(matchers, requests))
        connmark = connection.connmark
        def _wait_for_all():
            eventdict = {}
            conn_matcher = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.DOWN,
                                                                       connection,
                                                                       connmark)
            container.scheduler.register(matchers, container.currentroutine)
            try:
                ms = len(matchers)
                while ms:
                    yield (conn_matcher,)
                    if container.matcher is conn_matcher:
                        # Connection is down, but we should still receive any results already got
                        break
                    container.scheduler.unregister((container.matcher,), container.currentroutine)
                    ms -= 1
                    eventdict[container.matcher] = container.event
                    if callback:
                        callback(requests_dict[container.matcher], container.event.message)
            except:
                container.scheduler.unregister(matchers, container.currentroutine)
                raise
            else:
                container.retvalue = (not ms, [eventdict[m].message if m in eventdict else None for m in matchers])
        def _sent_all():
            try:
                for m in routine:
                    yield m
            except ZooKeeperRetryException as exc:
                container.retvalue = (False, exc.args[0])
            else:
                container.retvalue = (True, container.retvalue)
        for m in container.executeAll([_wait_for_all(),
                                       _sent_all()]):
            yield m
        # There are three possible results: not sent; sent but response lost; response received
        (((receive_all, responses),), ((_, sent_events),)) = container.retvalue
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

